package marcbp.trino.s3file.file;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;

/**
 * Utility for planning primary file splits whose readable range extends to the end of the object.
 *
 * <p>The tail range lets a reader finish the record crossing its primary boundary without imposing
 * a maximum record size. Page sources close the S3 stream as soon as that record has been emitted,
 * so the remaining tail is not consumed.</p>
 */
public final class SplitPlanner {
    private SplitPlanner() {}

    public static List<FileSplit> planSplits(long fileSize, int splitSizeBytes) {
        if (fileSize <= 0) {
            return List.of(FileSplit.forWholeFile(max(0, fileSize)));
        }

        if (splitSizeBytes <= 0) {
            throw new IllegalArgumentException("splitSizeBytes must be positive");
        }

        List<FileSplit> splits = new ArrayList<>();
        long offset = 0;
        int index = 0;
        while (offset < fileSize) {
            long primaryEnd = Math.min(fileSize, offset + splitSizeBytes);
            boolean last = primaryEnd >= fileSize;
            splits.add(new FileSplit(
                    "split-" + index,
                    offset,
                    primaryEnd,
                    fileSize,
                    offset == 0,
                    last));
            offset = primaryEnd;
            index++;
        }

        return List.copyOf(splits);
    }
}
