package marcbp.trino.s3file.s3;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility functions to work with S3 URIs.
 */
public final class S3UriUtils {
    public static final Pattern S3_URI = Pattern.compile("s3://([^/]+)/(.+)");
    private static final Pattern S3_PREFIX_URI = Pattern.compile("s3://([^/]+)(?:/(.*))?");

    public record S3Location(String bucket, String key) {}

    public static S3Location parse(String s3Uri) {
        Matcher matcher = S3_URI.matcher(s3Uri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }
        return new S3Location(matcher.group(1), matcher.group(2));
    }

    public static S3Location parsePrefix(String s3Uri) {
        Matcher matcher = S3_PREFIX_URI.matcher(s3Uri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }
        String key = matcher.group(2);
        return new S3Location(matcher.group(1), key == null ? "" : key);
    }
}
