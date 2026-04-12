package marcbp.trino.s3file.s3;

import java.time.Duration;
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
        int splitSizeBytes,
        int maxConnections,
        Duration connectionAcquisitionTimeout) {

    public static final String REGION_KEY = "s3.region";
    public static final String ENDPOINT_KEY = "s3.endpoint";
    public static final String ACCESS_KEY_KEY = "s3.access-key";
    public static final String SECRET_KEY_KEY = "s3.secret-key";
    public static final String PATH_STYLE_KEY = "s3.path-style-access";
    public static final String INTERCEPTOR_CLASS_KEY = "s3.interceptor-class";
    public static final String DEFAULT_SPLIT_SIZE_MB_KEY = "s3.default-split-size-mb";
    public static final String MAX_CONNECTIONS_KEY = "s3.max-connections-per-worker";
    public static final String CONNECTION_ACQUISITION_TIMEOUT_KEY = "s3.connection-acquisition-timeout-s";
    public static final int DEFAULT_SPLIT_SIZE_BYTES = 32 * 1024 * 1024;
    public static final int DEFAULT_MAX_CONNECTIONS = 5;
    public static final Duration DEFAULT_CONNECTION_ACQUISITION_TIMEOUT = Duration.ofSeconds(60);

    public static S3ClientConfig from(Map<String, String> config) {
        String region = optionalValue(config.get(REGION_KEY)).orElse("us-east-1");
        Optional<String> endpoint = optionalValue(config.get(ENDPOINT_KEY));
        Optional<String> accessKey = optionalValue(config.get(ACCESS_KEY_KEY));
        Optional<String> secretKey = optionalValue(config.get(SECRET_KEY_KEY));
        boolean pathStyleAccess = Boolean.parseBoolean(config.getOrDefault(PATH_STYLE_KEY, "true"));
        Optional<String> interceptorClass = optionalValue(config.get(INTERCEPTOR_CLASS_KEY));
        int splitSizeBytes = parseSplitSizeBytes(config.get(DEFAULT_SPLIT_SIZE_MB_KEY));
        int maxConnections = parseMaxConnections(config.get(MAX_CONNECTIONS_KEY));
        Duration connectionAcquisitionTimeout = parseConnectionAcquisitionTimeout(config.get(CONNECTION_ACQUISITION_TIMEOUT_KEY));
        return new S3ClientConfig(region, endpoint, accessKey, secretKey, pathStyleAccess, interceptorClass, splitSizeBytes, maxConnections, connectionAcquisitionTimeout);
    }

    public static S3ClientConfig defaults() {
        return new S3ClientConfig(
                "us-east-1",
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true,
                Optional.empty(),
                DEFAULT_SPLIT_SIZE_BYTES,
                DEFAULT_MAX_CONNECTIONS,
                DEFAULT_CONNECTION_ACQUISITION_TIMEOUT);
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

    private static int parseMaxConnections(String configuredMaxConnections) {
        Optional<String> maxConnections = optionalValue(configuredMaxConnections);
        if (maxConnections.isEmpty()) {
            return DEFAULT_MAX_CONNECTIONS;
        }

        try {
            int connections = Integer.parseInt(maxConnections.get());
            if (connections <= 0) {
                throw new IllegalArgumentException("Connector property " + MAX_CONNECTIONS_KEY + " must be a positive integer");
            }
            return connections;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Connector property " + MAX_CONNECTIONS_KEY + " must be a positive integer", e);
        }
    }

    private static Duration parseConnectionAcquisitionTimeout(String configuredConnectionAcquisitionTimeout) {
        Optional<String> timeout = optionalValue(configuredConnectionAcquisitionTimeout);
        if (timeout.isEmpty()) {
            return DEFAULT_CONNECTION_ACQUISITION_TIMEOUT;
        }

        try {
            long seconds = Long.parseLong(timeout.get());
            if (seconds <= 0) {
                throw new IllegalArgumentException("Connector property " + CONNECTION_ACQUISITION_TIMEOUT_KEY + " must be a positive integer");
            }
            return Duration.ofSeconds(seconds);
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Connector property " + CONNECTION_ACQUISITION_TIMEOUT_KEY + " must be a positive integer", e);
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
