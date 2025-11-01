package marcbp.trino.s3file.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility functions to work with S3 URIs.
 */
public final class S3UriUtils {
    public static final Pattern S3_URI = Pattern.compile("s3://([^/]+)/(.+)");

    public record S3Location(String bucket, String key) {}

    public static S3Location parse(String s3Uri) {
        Matcher matcher = S3_URI.matcher(s3Uri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }
        return new S3Location(matcher.group(1), matcher.group(2));
    }
}
