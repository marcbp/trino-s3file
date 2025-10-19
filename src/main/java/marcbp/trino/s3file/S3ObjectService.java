package marcbp.trino.s3file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Provides low-level S3 access helpers shared by the table functions.
 */
public final class S3ObjectService implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(S3ObjectService.class);
    private static final Pattern S3_URI = Pattern.compile("s3://([^/]+)/(.+)");

    private final S3Client s3Client;

    public S3ObjectService() {
        this(buildClientFromEnvironment());
    }

    public S3ObjectService(S3Client s3Client) {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
    }

    public BufferedReader openReader(String s3Uri) {
        return openReader(s3Uri, 0, null);
    }

    public BufferedReader openReader(String s3Uri, long start, Long endExclusive) {
        S3Location location = parseLocation(s3Uri);
        GetObjectRequest.Builder builder = GetObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key());
        if (start > 0 || (endExclusive != null && endExclusive >= 0)) {
            if (endExclusive != null && endExclusive < start) {
                endExclusive = start;
            }
            StringBuilder range = new StringBuilder("bytes=").append(start).append("-");
            if (endExclusive != null && endExclusive >= 0) {
                long inclusiveEnd = Math.max(start, endExclusive - 1);
                range.append(inclusiveEnd);
            }
            builder.range(range.toString());
        }
        ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(builder.build());
        LOG.info("Opened S3 object {}/{}", location.bucket(), location.key());
        return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }

    public long getObjectSize(String s3Uri) {
        S3Location location = parseLocation(s3Uri);
        HeadObjectResponse response = s3Client.headObject(HeadObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .build());
        return response.contentLength();
    }

    @Override
    public void close() {
        s3Client.close();
    }

    private static S3Location parseLocation(String s3Uri) {
        Matcher matcher = S3_URI.matcher(s3Uri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }
        return new S3Location(matcher.group(1), matcher.group(2));
    }

    private static S3Client buildClientFromEnvironment() {
        Map<String, String> env = System.getenv();
        S3ClientBuilder builder = S3Client.builder();

        String regionValue = env.getOrDefault("AWS_REGION", "us-east-1");
        builder.region(Region.of(regionValue));

        String endpoint = env.get("AWS_S3_ENDPOINT");
        if (endpoint != null && !endpoint.isBlank()) {
            LOG.info("Using custom S3 endpoint: {}", endpoint);
            try {
                builder.endpointOverride(new URI(endpoint));
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid S3 endpoint: " + endpoint, e);
            }
        }

        String accessKey = env.get("AWS_ACCESS_KEY_ID");
        String secretKey = env.get("AWS_SECRET_ACCESS_KEY");
        AwsCredentialsProvider credentialsProvider;
        if (accessKey != null && secretKey != null) {
            LOG.info("Using explicit AWS credentials");
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }
        else {
            LOG.info("Using default AWS credentials provider chain");
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        builder.credentialsProvider(credentialsProvider);

        builder.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());

        S3Client client = builder.build();
        LOG.info("Initialized S3 client for region {}", client.serviceClientConfiguration().region());
        return client;
    }

    private record S3Location(String bucket, String key) {}
}
