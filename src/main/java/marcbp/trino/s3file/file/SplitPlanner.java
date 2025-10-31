package marcbp.trino.s3file.file;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;

/**
 * Utility for planning file splits with optional lookahead bytes to finish partial records.
 */
public final class SplitPlanner {
    private SplitPlanner() {}

    public static List<FileSplit> planSplits(long fileSize, int splitSizeBytes, int lookaheadBytes) {
        if (fileSize <= 0) {
            return List.of(FileSplit.forWholeFile(max(0, fileSize)));
        }

        if (splitSizeBytes <= 0) {
            throw new IllegalArgumentException("splitSizeBytes must be positive");
        }

        if (lookaheadBytes < 0) {
            throw new IllegalArgumentException("lookaheadBytes must be >= 0");
        }

        List<FileSplit> splits = new ArrayList<>();
        long offset = 0;
        int index = 0;
        while (offset < fileSize) {
            long primaryEnd = Math.min(fileSize, offset + splitSizeBytes);
            boolean last = primaryEnd >= fileSize;
            long rangeEnd = last ? fileSize : Math.min(fileSize, primaryEnd + lookaheadBytes);
            splits.add(new FileSplit(
                    "split-" + index,
                    offset,
                    primaryEnd,
                    rangeEnd,
                    offset == 0,
                    last));
            offset = primaryEnd;
            index++;
        }
        
        return List.copyOf(splits);
    }
}
