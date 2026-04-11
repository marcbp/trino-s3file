package marcbp.trino.s3file.s3;

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
        boolean pathStyleAccess,
        Optional<String> interceptorClass,
        int splitSizeBytes) {

    public static final String REGION_KEY = "s3.region";
    public static final String ENDPOINT_KEY = "s3.endpoint";
    public static final String ACCESS_KEY_KEY = "s3.access-key";
    public static final String SECRET_KEY_KEY = "s3.secret-key";
    public static final String PATH_STYLE_KEY = "s3.path-style-access";
    public static final String INTERCEPTOR_CLASS_KEY = "s3.interceptor-class";
    public static final String DEFAULT_SPLIT_SIZE_MB_KEY = "s3.default-split-size-mb";
    public static final int DEFAULT_SPLIT_SIZE_BYTES = 32 * 1024 * 1024;

    public static S3ClientConfig from(Map<String, String> config) {
        String region = optionalValue(config.get(REGION_KEY)).orElse("us-east-1");
        Optional<String> endpoint = optionalValue(config.get(ENDPOINT_KEY));
        Optional<String> accessKey = optionalValue(config.get(ACCESS_KEY_KEY));
        Optional<String> secretKey = optionalValue(config.get(SECRET_KEY_KEY));
        boolean pathStyleAccess = Boolean.parseBoolean(config.getOrDefault(PATH_STYLE_KEY, "true"));
        Optional<String> interceptorClass = optionalValue(config.get(INTERCEPTOR_CLASS_KEY));
        int splitSizeBytes = parseSplitSizeBytes(config.get(DEFAULT_SPLIT_SIZE_MB_KEY));
        return new S3ClientConfig(region, endpoint, accessKey, secretKey, pathStyleAccess, interceptorClass, splitSizeBytes);
    }

    public static S3ClientConfig defaults() {
        return new S3ClientConfig("us-east-1", Optional.empty(), Optional.empty(), Optional.empty(), true, Optional.empty(), DEFAULT_SPLIT_SIZE_BYTES);
    }

    private static int parseSplitSizeBytes(String configuredSplitSizeMb) {
        Optional<String> splitSizeMb = optionalValue(configuredSplitSizeMb);
        if (splitSizeMb.isEmpty()) {
            return DEFAULT_SPLIT_SIZE_BYTES;
        }

        try {
            long megabytes = Long.parseLong(splitSizeMb.get());
            if (megabytes <= 0) {
                throw new IllegalArgumentException("Connector property " + DEFAULT_SPLIT_SIZE_MB_KEY + " must be a positive integer");
            }
            long bytes = Math.multiplyExact(megabytes, 1024L * 1024L);
            if (bytes > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Connector property " + DEFAULT_SPLIT_SIZE_MB_KEY + " is too large");
            }
            return (int) bytes;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Connector property " + DEFAULT_SPLIT_SIZE_MB_KEY + " must be a positive integer", e);
        }
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
