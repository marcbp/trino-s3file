package marcbp.trino.s3file.s3;

import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorSession;
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
import software.amazon.awssdk.http.apache.ApacheHttpClient;
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
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import marcbp.trino.s3file.s3.S3UriUtils.S3Location;

import static java.util.Objects.requireNonNull;

/**
 * Builder for query-scoped S3 clients with optional execution interceptors.
 */
public final class S3ClientBuilder implements Closeable {
    public static final ExecutionAttribute<ConnectorSession> SESSION_ATTRIBUTE =
            new ExecutionAttribute<>("trino.connector.session");
    private static final long IDLE_EVICTION_DELAY_SECONDS = 5L * 60L;

    public record ObjectMetadata(long size, Optional<String> eTag, Optional<String> versionId) {}

    private final Logger logger = Logger.get(S3ClientBuilder.class);
    private final S3ClientConfig config;
    private final ConcurrentHashMap<String, CachedS3Client> clients = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public S3ClientBuilder(S3ClientConfig clientConfig) {
        this.config = requireNonNull(clientConfig, "clientConfig is null");
    }

    public SessionClient forSession(ConnectorSession session) {
        if (closed.get()) {
            throw new IllegalStateException("S3ClientBuilder is closed");
        }
        return new SessionClient(session);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        clients.values().forEach(CachedS3Client::forceClose);
        clients.clear();
    }

    private CachedS3Client acquire(ConnectorSession session) {
        evictIdleEntries();
        String queryId = requireNonNull(session.getQueryId(), "queryId is null");
        return clients.compute(queryId, (id, existing) -> {
            if (existing == null) {
                return new CachedS3Client(createClient(session));
            }
            existing.acquire();
            return existing;
        });
    }

    private void release(String queryId, CachedS3Client entry) {
        entry.release();
        entry.touch();
    }

    private void evictIdleEntries() {
        long now = System.currentTimeMillis() / 1000L;
        AtomicInteger evicted = new AtomicInteger();
        clients.forEach((queryId, entry) -> {
            if (entry.isExpired(now, IDLE_EVICTION_DELAY_SECONDS) && clients.remove(queryId, entry)) {
                entry.forceClose();
                evicted.incrementAndGet();
            }
        });
        if (evicted.get() > 0) {
            logger.info("Evicted %s idle S3 client(s) from cache", evicted.get());
        }
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
            credentialsProvider = DefaultCredentialsProvider.builder().build();
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
        builder.httpClientBuilder(ApacheHttpClient.builder()
                .maxConnections(config.maxConnections())
                .connectionAcquisitionTimeout(config.connectionAcquisitionTimeout()));

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

    public final class SessionClient implements Closeable {
        private final String queryId;
        private final CachedS3Client entry;
        private boolean closed;

        private SessionClient(ConnectorSession session) {
            this.queryId = session.getQueryId();
            this.entry = acquire(session);
        }

        public BufferedReader openReader(String s3Uri) {
            return openReader(s3Uri, StandardCharsets.UTF_8);
        }

        public BufferedReader openReader(String s3Uri, Charset charset) {
            return openReader(s3Uri, 0, null, charset, Optional.empty(), Optional.empty());
        }

        public BufferedReader openReader(String s3Uri, long start, Long endExclusive) {
            return openReader(s3Uri, start, endExclusive, StandardCharsets.UTF_8, Optional.empty(), Optional.empty());
        }

        public BufferedReader openReader(String s3Uri, long start, Long endExclusive, Charset charset) {
            return openReader(s3Uri, start, endExclusive, charset, Optional.empty(), Optional.empty());
        }

        public BufferedReader openReader(String s3Uri, Charset charset, Optional<String> versionId, Optional<String> eTag) {
            return openReader(s3Uri, 0, null, charset, versionId, eTag);
        }

        public BufferedReader openReader(String s3Uri, long start, Long endExclusive, Charset charset, Optional<String> versionId, Optional<String> eTag) {
            ResponseInputStream<GetObjectResponse> stream = openObjectStream(s3Uri, start, endExclusive, versionId, eTag);
            logger.info("Opened S3 object %s/%s (versionId=%s, etag=%s)",
                    S3UriUtils.parse(s3Uri).bucket(),
                    S3UriUtils.parse(s3Uri).key(),
                    versionId.orElse("n/a"),
                    eTag.orElse("n/a"));

            return new ClosingBufferedReader(new InputStreamReader(stream, charset), stream);
        }

        public byte[] readBytes(String s3Uri, long start, Long endExclusive, Optional<String> versionId, Optional<String> eTag)
                throws IOException {
            try (ResponseInputStream<GetObjectResponse> stream = openObjectStream(s3Uri, start, endExclusive, versionId, eTag)) {
                return stream.readAllBytes();
            }
        }

        public ObjectMetadata getObjectMetadata(String s3Uri) {
            S3Location location = S3UriUtils.parse(s3Uri);
            HeadObjectResponse response = entry.client.headObject(HeadObjectRequest.builder()
                    .bucket(location.bucket())
                    .key(location.key())
                    .build());
            return new ObjectMetadata(
                    response.contentLength(),
                    Optional.ofNullable(response.eTag()),
                    Optional.ofNullable(response.versionId()));
        }

        public long getObjectSize(String s3Uri) {
            return getObjectMetadata(s3Uri).size();
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            release(queryId, entry);
        }

        private ResponseInputStream<GetObjectResponse> openObjectStream(
                String s3Uri,
                long start,
                Long endExclusive,
                Optional<String> versionId,
                Optional<String> eTag) {
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

            versionId.ifPresentOrElse(
                    requestBuilder::versionId,
                    () -> eTag.ifPresent(requestBuilder::ifMatch));

            return entry.client.getObject(requestBuilder.build());
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

    private final class CachedS3Client {
        private final S3Client client;
        private volatile long lastUsedAtSeconds = System.currentTimeMillis() / 1000L;
        private int references = 1;
        private boolean closed;

        private CachedS3Client(S3Client client) {
            this.client = requireNonNull(client, "client is null");
        }

        private synchronized void acquire() {
            if (closed) {
                throw new IllegalStateException("S3 client entry is closed");
            }
            references++;
            lastUsedAtSeconds = System.currentTimeMillis() / 1000L;
        }

        private synchronized void release() {
            if (references <= 0) {
                throw new IllegalStateException("S3 client entry reference count underflow");
            }
            references--;
            lastUsedAtSeconds = System.currentTimeMillis() / 1000L;
        }

        private synchronized void touch() {
            lastUsedAtSeconds = System.currentTimeMillis() / 1000L;
        }

        private synchronized boolean isExpired(long nowSeconds, long ttlSeconds) {
            if (references < 0) {
                throw new IllegalStateException("S3 client entry reference count must not be negative");
            }
            return !closed && references == 0 && nowSeconds - lastUsedAtSeconds >= ttlSeconds;
        }

        private void forceClose() {
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
            }
            client.close();
        }
    }

}
