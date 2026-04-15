package marcbp.trino.s3file.file;

import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.IOException;

/**
 * Helpers for deciding whether a text split starts on a logical record boundary.
 */
public final class TextSplitBoundarySupport {
    private TextSplitBoundarySupport() {}

    public static boolean startsAtLineBoundary(
            S3ClientBuilder.SessionClient sessionClient,
            BaseTextFileHandle handle,
            FileSplit split)
            throws IOException {
        if (split.isFirst()) {
            return true;
        }
        byte[] preceding = sessionClient.readBytes(
                handle.object().path(),
                split.getStartOffset() - 1,
                split.getStartOffset(),
                handle.object().versionIdRef(),
                handle.object().eTagRef());
        return preceding.length == 1 && (preceding[0] == '\n' || preceding[0] == '\r');
    }

    public static boolean startsAfterDelimiter(
            S3ClientBuilder.SessionClient sessionClient,
            BaseTextFileHandle handle,
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
                handle.object().path(),
                lookbehindStart,
                split.getStartOffset(),
                handle.object().versionIdRef(),
                handle.object().eTagRef());
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
