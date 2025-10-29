package marcbp.trino.s3file.util;

import io.trino.spi.connector.ConnectorSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Builder for session-scoped S3 clients with optional execution interceptors.
 */
public final class S3ClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(S3ClientBuilder.class);
    private static final Pattern S3_URI = Pattern.compile("s3://([^/]+)/(.+)");
    private static final ExecutionAttribute<ConnectorSession> SESSION_ATTRIBUTE =
            new ExecutionAttribute<>("trino.connector.session");

    private final S3ClientConfig config;
    private final ExecutionInterceptor userInterceptor;

    public S3ClientBuilder() {
        this(S3ClientConfig.defaults());
    }

    public S3ClientBuilder(S3ClientConfig clientConfig) {
        this.config = requireNonNull(clientConfig, "clientConfig is null");
        this.userInterceptor = clientConfig.interceptorClass()
                .map(S3ClientBuilder::instantiateInterceptor)
                .orElse(null);
    }

    public static ExecutionAttribute<ConnectorSession> sessionAttribute() {
        return SESSION_ATTRIBUTE;
    }

    public SessionClient forSession(ConnectorSession session) {
        return new SessionClient(session, createClient(session));
    }

    private S3Client createClient(ConnectorSession session) {
        software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder();

        builder.region(Region.of(config.region()));

        config.endpoint().ifPresent(endpoint -> {
            LOG.info("Using custom S3 endpoint: {}", endpoint);
            try {
                builder.endpointOverride(new URI(endpoint));
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid S3 endpoint: " + endpoint, e);
            }
        });

        if (config.accessKey().isPresent() ^ config.secretKey().isPresent()) {
            throw new IllegalArgumentException("Both aws.access-key and aws.secret-key must be provided together");
        }

        AwsCredentialsProvider credentialsProvider;
        if (config.accessKey().isPresent()) {
            LOG.info("Using static AWS credentials from connector configuration");
            credentialsProvider = StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.accessKey().get(), config.secretKey().get()));
        }
        else {
            LOG.info("Using AWS default credentials provider chain");
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        builder.credentialsProvider(credentialsProvider);

        builder.overrideConfiguration(b -> b.addExecutionInterceptor(new SessionAttributeInterceptor(session)));

        if (userInterceptor != null) {
            builder.overrideConfiguration(b -> b.addExecutionInterceptor(userInterceptor));
        }

        builder.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(config.pathStyleAccess())
                .build());

        return builder.build();
    }

    private static ExecutionInterceptor instantiateInterceptor(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            if (!ExecutionInterceptor.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException("Class " + className + " must implement " + ExecutionInterceptor.class.getName());
            }
            return (ExecutionInterceptor) clazz.getDeclaredConstructor().newInstance();
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Execution interceptor class not found: " + className, e);
        }
        catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException("Failed to instantiate execution interceptor: " + className, e);
        }
    }

    private static S3Location parseLocation(String s3Uri) {
        Matcher matcher = S3_URI.matcher(s3Uri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }
        return new S3Location(matcher.group(1), matcher.group(2));
    }

    private static final class SessionAttributeInterceptor implements ExecutionInterceptor {
        private final ConnectorSession session;

        private SessionAttributeInterceptor(ConnectorSession session) {
            this.session = session;
        }

        @Override
        public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
            executionAttributes.putAttribute(SESSION_ATTRIBUTE, session);
        }
    }

    private record S3Location(String bucket, String key) {}

    public final class SessionClient implements Closeable {
        private final ConnectorSession session;
        private final S3Client client;

        private SessionClient(ConnectorSession session, S3Client client) {
            this.session = requireNonNull(session, "session is null");
            this.client = requireNonNull(client, "client is null");
        }

        public BufferedReader openReader(String s3Uri) {
            return openReader(s3Uri, StandardCharsets.UTF_8);
        }

        public BufferedReader openReader(String s3Uri, Charset charset) {
            return openReader(s3Uri, 0, null, charset);
        }

        public BufferedReader openReader(String s3Uri, long start, Long endExclusive) {
            return openReader(s3Uri, start, endExclusive, StandardCharsets.UTF_8);
        }

        public BufferedReader openReader(String s3Uri, long start, Long endExclusive, Charset charset) {
            S3Location location = parseLocation(s3Uri);
            GetObjectRequest.Builder requestBuilder = GetObjectRequest.builder()
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
                requestBuilder.range(range.toString());
            }

            ResponseInputStream<GetObjectResponse> stream = client.getObject(requestBuilder.build());
            LOG.info("Opened S3 object {}/{}", location.bucket(), location.key());
            return new ClosingBufferedReader(new InputStreamReader(stream, charset), stream);
        }

        public long getObjectSize(String s3Uri) {
            S3Location location = parseLocation(s3Uri);
            HeadObjectResponse response = client.headObject(HeadObjectRequest.builder()
                    .bucket(location.bucket())
                    .key(location.key())
                    .build());
            return response.contentLength();
        }

        @Override
        public void close() {
            client.close();
        }

        private final class ClosingBufferedReader extends BufferedReader {
            private final ResponseInputStream<GetObjectResponse> stream;

            private ClosingBufferedReader(InputStreamReader reader,
                                          ResponseInputStream<GetObjectResponse> stream) {
                super(reader);
                this.stream = stream;
            }

            @Override
            public void close() throws IOException {
                try (stream) {
                    super.close();
                }
            }
        }
    }
}
