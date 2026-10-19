package marcbp.trino.s3file.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SplitPlannerTest {

    @Test
    void splitsRoundTripWithoutDerivedProperties() throws Exception {
        var object = new S3ObjectRef("s3://bucket/data.gz", 42, "etag", "version", Compression.GZIP);
        var split = FileSplit.forWholeFile(object);
        var mapper = new ObjectMapper();
        assertEquals(split, mapper.readValue(mapper.writeValueAsBytes(split), FileSplit.class));
        var range = SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/data", 42, null, null, Compression.NONE), 10).get(1);
        assertEquals(range, mapper.readValue(mapper.writeValueAsBytes(range), FileSplit.class));
    }

    @Test
    void zeroOrNegativeSizeProducesWholeFileSplit() {
        List<FileSplit> zeroSplits = SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/file", 0, null, null, Compression.NONE), 8 * 1024 * 1024);
        assertEquals(1, zeroSplits.size());
        assertEquals(0, zeroSplits.get(0).primaryLength());

        List<FileSplit> negativeSplits = SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/file", -42, null, null, Compression.NONE), 8 * 1024 * 1024);
        assertEquals(1, negativeSplits.size());
        assertEquals(0, negativeSplits.get(0).primaryLength());
    }

    @Test
    void singleSplitWhenFileFitsIntoChunk() {
        List<FileSplit> splits = SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/file", 1_000_000, null, null, Compression.NONE), 8 * 1024 * 1024);
        assertEquals(1, splits.size());
        FileSplit split = splits.get(0);
        assertTrue(split.first());
        assertTrue(split.last());
        assertEquals(0, split.startOffset());
        assertEquals(1_000_000, split.primaryEndOffset());
        assertEquals(split.primaryEndOffset(), split.rangeEndExclusive());
    }

    @Test
    void multipleSplitsCanReadTailToFinishBoundaryRecord() {
        int splitSize = 4;
        long fileSize = 11;

        List<FileSplit> splits = SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/file", fileSize, null, null, Compression.NONE), splitSize);
        assertEquals(3, splits.size());

        FileSplit first = splits.get(0);
        assertEquals(0, first.startOffset());
        assertEquals(4, first.primaryEndOffset());
        assertEquals(fileSize, first.rangeEndExclusive());
        assertTrue(first.first());
        assertTrue(first.primaryLength() > 0);

        FileSplit second = splits.get(1);
        assertEquals(4, second.startOffset());
        assertEquals(8, second.primaryEndOffset());
        assertEquals(fileSize, second.rangeEndExclusive());
        assertTrue(second.primaryLength() > 0);

        FileSplit third = splits.get(2);
        assertEquals(8, third.startOffset());
        assertEquals(11, third.primaryEndOffset());
        assertEquals(11, third.rangeEndExclusive());
        assertTrue(third.last());
    }

    @Test
    void splitSizeMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/file", 100, null, null, Compression.NONE), 0));
        assertThrows(IllegalArgumentException.class, () -> SplitPlanner.planSplits(new S3ObjectRef("s3://bucket/file", 100, null, null, Compression.NONE), -1));
    }

}
