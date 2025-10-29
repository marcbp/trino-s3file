package marcbp.trino.s3file.util;

import java.util.Map;
import java.util.Optional;

/**
 * Configuration holder for building the S3 client.
 */
public record S3ClientConfig(
        String region,
        Optional<String> endpoint,
        Optional<String> accessKey,
        Optional<String> secretKey,
        boolean pathStyleAccess) {

    public static final String REGION_KEY = "aws.region";
    public static final String ENDPOINT_KEY = "aws.endpoint";
    public static final String ACCESS_KEY_KEY = "aws.access-key";
    public static final String SECRET_KEY_KEY = "aws.secret-key";
    public static final String PATH_STYLE_KEY = "aws.path-style-access";

    public static S3ClientConfig from(Map<String, String> config) {
        String region = config.getOrDefault(REGION_KEY, "us-east-1").trim();
        Optional<String> endpoint = optionalValue(config.get(ENDPOINT_KEY));
        Optional<String> accessKey = optionalValue(config.get(ACCESS_KEY_KEY));
        Optional<String> secretKey = optionalValue(config.get(SECRET_KEY_KEY));
        boolean pathStyleAccess = Boolean.parseBoolean(config.getOrDefault(PATH_STYLE_KEY, "true"));
        return new S3ClientConfig(region, endpoint, accessKey, secretKey, pathStyleAccess);
    }

    public static S3ClientConfig defaults() {
        return new S3ClientConfig("us-east-1", Optional.empty(), Optional.empty(), Optional.empty(), true);
    }

    private static Optional<String> optionalValue(String value) {
        if (value == null) {
            return Optional.empty();
        }
        String trimmed = value.trim();
        if (trimmed.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(trimmed);
    }
}
