package marcbp.trino.s3file.util;

import io.trino.spi.connector.ConnectorSession;
import io.airlift.log.Logger;
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

import marcbp.trino.s3file.util.S3UriUtils.S3Location;

import static java.util.Objects.requireNonNull;

/**
 * Builder for session-scoped S3 clients with optional execution interceptors.
 */
public final class S3ClientBuilder {
    public static final ExecutionAttribute<ConnectorSession> SESSION_ATTRIBUTE =
            new ExecutionAttribute<>("trino.connector.session");

    private final Logger logger = Logger.get(S3ClientBuilder.class);
    private final S3ClientConfig config;

    public S3ClientBuilder(S3ClientConfig clientConfig) {
        this.config = requireNonNull(clientConfig, "clientConfig is null");
    }

    public SessionClient forSession(ConnectorSession session) {
        return new SessionClient(session);
    }

    public final class SessionClient implements Closeable {
        private final S3Client client;

        private SessionClient(ConnectorSession session) {
            this.client = createClient(session);
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
            S3Location location = S3UriUtils.parse(s3Uri);
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
            logger.info("Opened S3 object %s/%s", location.bucket(), location.key());
            return new ClosingBufferedReader(new InputStreamReader(stream, charset), stream);
        }

        public long getObjectSize(String s3Uri) {
            S3Location location = S3UriUtils.parse(s3Uri);
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

        private S3Client createClient(ConnectorSession session) {
            software.amazon.awssdk.services.s3.S3ClientBuilder builder = S3Client.builder();

            builder.region(Region.of(config.region()));

            config.endpoint().ifPresent(endpoint -> {
                logger.info("Using custom S3 endpoint: %s", endpoint);
                try {
                    builder.endpointOverride(new URI(endpoint));
                }
                catch (URISyntaxException e) {
                    throw new IllegalArgumentException("Invalid S3 endpoint: " + endpoint, e);
                }
            });

            if (config.accessKey().isPresent() ^ config.secretKey().isPresent()) {
                throw new IllegalArgumentException("Both s3.access-key and s3.secret-key must be provided together");
            }

            AwsCredentialsProvider credentialsProvider;
            if (config.accessKey().isPresent()) {
                logger.info("Using static AWS credentials from connector configuration");
                credentialsProvider = StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(config.accessKey().get(), config.secretKey().get()));
            }
            else {
                logger.info("Using AWS default credentials provider chain");
                credentialsProvider = DefaultCredentialsProvider.create();
            }
            builder.credentialsProvider(credentialsProvider);
            
            config.interceptorClass().ifPresent(className -> {
                builder.overrideConfiguration(c -> {
                    c.addExecutionInterceptor(new SessionAttributeInterceptor(session));
                    c.addExecutionInterceptor(instantiateInterceptor(className));
                    logger.debug("Registered execution interceptor %s", className);
                });
            });

            builder.serviceConfiguration(S3Configuration.builder()
                    .pathStyleAccessEnabled(config.pathStyleAccess())
                    .build());

            return builder.build();
        }

        private ExecutionInterceptor instantiateInterceptor(String className) {
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

        private final class SessionAttributeInterceptor implements ExecutionInterceptor {
            private final ConnectorSession session;

            private SessionAttributeInterceptor(ConnectorSession session) {
                this.session = session;
            }

            @Override
            public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
                executionAttributes.putAttribute(SESSION_ATTRIBUTE, session);
            }
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
