package marcbp.trino.s3file.file;

import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.IOException;

/**
 * Helpers for deciding whether a split starts on a logical record boundary.
 */
public final class SplitBoundarySupport {
    private SplitBoundarySupport() {}

    public static boolean startsAtLineBoundary(
            S3ClientBuilder.SessionClient sessionClient,
            BaseFileHandle handle,
            FileSplit split)
            throws IOException {
        if (split.isFirst()) {
            return true;
        }
        byte[] preceding = sessionClient.readBytes(
                handle.getS3Path(),
                split.getStartOffset() - 1,
                split.getStartOffset(),
                handle.getVersionId(),
                handle.getETag());
        return preceding.length == 1 && (preceding[0] == '\n' || preceding[0] == '\r');
    }

    public static boolean startsAfterDelimiter(
            S3ClientBuilder.SessionClient sessionClient,
            BaseFileHandle handle,
            FileSplit split,
            byte[] delimiterBytes)
            throws IOException {
        if (split.isFirst()) {
            return true;
        }
        if (delimiterBytes.length == 0 || split.getStartOffset() < delimiterBytes.length) {
            return false;
        }
        long lookbehindStart = split.getStartOffset() - delimiterBytes.length;
        byte[] preceding = sessionClient.readBytes(
                handle.getS3Path(),
                lookbehindStart,
                split.getStartOffset(),
                handle.getVersionId(),
                handle.getETag());
        if (preceding.length != delimiterBytes.length) {
            return false;
        }
        for (int i = 0; i < delimiterBytes.length; i++) {
            if (preceding[i] != delimiterBytes[i]) {
                return false;
            }
        }
        return true;
    }
}
