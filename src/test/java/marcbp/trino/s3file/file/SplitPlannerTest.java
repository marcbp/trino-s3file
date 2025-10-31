package marcbp.trino.s3file.file;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitPlannerTest {

    @Test
    void zeroOrNegativeSizeProducesWholeFileSplit() {
        List<FileSplit> zeroSplits = SplitPlanner.planSplits(0, 8 * 1024 * 1024, 0);
        assertEquals(1, zeroSplits.size());
        assertEquals(0, zeroSplits.get(0).getPrimaryLength());

        List<FileSplit> negativeSplits = SplitPlanner.planSplits(-42, 8 * 1024 * 1024, 0);
        assertEquals(1, negativeSplits.size());
        assertEquals(0, negativeSplits.get(0).getPrimaryLength());
    }

    @Test
    void singleSplitWhenFileFitsIntoChunk() {
        List<FileSplit> splits = SplitPlanner.planSplits(1_000_000, 8 * 1024 * 1024, 256 * 1024);
        assertEquals(1, splits.size());
        FileSplit split = splits.get(0);
        assertTrue(split.isFirst());
        assertTrue(split.isLast());
        assertEquals(0, split.getStartOffset());
        assertEquals(1_000_000, split.getPrimaryEndOffset());
        assertEquals(split.getPrimaryEndOffset(), split.getRangeEndExclusive());
    }

    @Test
    void multipleSplitsRespectLookahead() {
        int splitSize = 4;
        int lookahead = 2;
        long fileSize = 11;

        List<FileSplit> splits = SplitPlanner.planSplits(fileSize, splitSize, lookahead);
        assertEquals(3, splits.size());

        FileSplit first = splits.get(0);
        assertEquals(0, first.getStartOffset());
        assertEquals(4, first.getPrimaryEndOffset());
        assertEquals(6, first.getRangeEndExclusive());
        assertTrue(first.isFirst());
        assertTrue(first.getPrimaryLength() > 0);

        FileSplit second = splits.get(1);
        assertEquals(4, second.getStartOffset());
        assertEquals(8, second.getPrimaryEndOffset());
        assertEquals(10, second.getRangeEndExclusive());
        assertTrue(second.getPrimaryLength() > 0);

        FileSplit third = splits.get(2);
        assertEquals(8, third.getStartOffset());
        assertEquals(11, third.getPrimaryEndOffset());
        assertEquals(11, third.getRangeEndExclusive());
        assertTrue(third.isLast());
    }

    @Test
    void splitSizeMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> SplitPlanner.planSplits(100, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> SplitPlanner.planSplits(100, -1, 0));
    }

    @Test
    void lookaheadMustBeNonNegative() {
        assertThrows(IllegalArgumentException.class, () -> SplitPlanner.planSplits(100, 10, -1));
    }
}
