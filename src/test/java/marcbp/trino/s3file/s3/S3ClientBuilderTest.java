package marcbp.trino.s3file.s3;

import com.sun.net.httpserver.HttpServer;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import marcbp.trino.s3file.file.Compression;
import marcbp.trino.s3file.file.FileSelection;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.PageSettings;
import marcbp.trino.s3file.file.S3ObjectRef;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.txt.TextTableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ClientBuilderTest {
    private final BlockingQueue<Map<String, String>> requests = new LinkedBlockingQueue<>();
    private HttpServer server;
    private ExecutorService executor;
    private S3ClientBuilder builder;
    private ConnectorSession session;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newVirtualThreadPerTaskExecutor();
        server.setExecutor(executor);
        server.createContext("/bucket/data.txt", exchange -> {
            try (exchange) {
                String ifMatch = exchange.getRequestHeaders().getFirst("If-Match");
                requests.add(Map.of(
                        "range", Optional.ofNullable(exchange.getRequestHeaders().getFirst("Range")).orElse(""),
                        "ifMatch", Optional.ofNullable(ifMatch).orElse(""),
                        "query", Optional.ofNullable(exchange.getRequestURI().getRawQuery()).orElse("")));
                if (ifMatch != null && !ifMatch.equals("\"current\"")) {
                    exchange.sendResponseHeaders(412, -1);
                }
                else {
                    exchange.sendResponseHeaders(200, 3);
                    exchange.getResponseBody().write(new byte[] {'a', 'b', 'c'});
                }
            }
        });
        server.start();
        configureClient(Map.of());
        session = mock(ConnectorSession.class);
        when(session.getQueryId()).thenReturn("query-1");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (builder != null) {
            builder.close();
        }
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "test threads must terminate");
        }
    }

    @Test
    void rangedReadsUseExclusiveEndAndPinnedEtag() throws Exception {
        try (var client = builder.forSession(session);
                InputStream stream = client.openStream("s3://bucket/data.txt", 2, 5L,
                        Optional.empty(), Optional.of("\"current\""))) {
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, stream.readAllBytes());
        }
        assertEquals(Map.of("range", "bytes=2-4", "ifMatch", "\"current\"", "query", ""),
                requests.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void versionIdTakesPrecedenceOverEtag() throws Exception {
        try (var client = builder.forSession(session)) {
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, client.readBytes(
                    "s3://bucket/data.txt", 0, null, Optional.of("v1"), Optional.of("\"stale\"")));
        }
        assertEquals(Map.of("range", "", "ifMatch", "", "query", "versionId=v1"),
                requests.poll(5, TimeUnit.SECONDS));
    }

    @Test
    void changedObjectFailsInsteadOfReadingNewContents() {
        try (var client = builder.forSession(session)) {
            S3Exception error = assertThrows(S3Exception.class, () -> client.openStream(
                    "s3://bucket/data.txt", 0, null, Optional.empty(), Optional.of("\"stale\"")));
            assertEquals(412, error.statusCode());
        }
    }

    @ParameterizedTest
    @CsvSource({"GET,404,NoSuchKey", "GET,403,AccessDenied", "HEAD,404,NoSuchKey", "HEAD,403,AccessDenied"})
    void s3ErrorsPreserveDetailsAndReleaseConnection(String method, int status, String code) throws Exception {
        server.createContext("/bucket/error.txt", exchange -> {
            try (exchange) {
                exchange.getResponseHeaders().add("x-amz-request-id", "s3-error-request");
                exchange.getResponseHeaders().add("Content-Type", "application/xml");
                if (exchange.getRequestMethod().equals("HEAD")) {
                    exchange.sendResponseHeaders(status, -1);
                }
                else {
                    byte[] body = ("<Error><Code>" + code + "</Code><Message>Cannot read object</Message></Error>")
                            .getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(status, body.length);
                    exchange.getResponseBody().write(body);
                }
            }
        });

        try (var client = builder.forSession(session)) {
            S3Exception error = assertThrows(S3Exception.class, () -> {
                if (method.equals("HEAD")) {
                    client.getObjectMetadata("s3://bucket/error.txt");
                }
                else {
                    try (InputStream stream = client.openStream(
                            "s3://bucket/error.txt", 0, null, Optional.empty(), Optional.empty())) {
                        stream.readAllBytes();
                    }
                }
            });
            assertEquals(status, error.statusCode());
            assertEquals("s3-error-request", error.requestId());
            if (method.equals("GET")) {
                assertEquals(code, error.awsErrorDetails().errorCode());
                assertEquals("Cannot read object", error.awsErrorDetails().errorMessage());
            }
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, client.readBytes(
                    "s3://bucket/data.txt", 0, null, Optional.empty(), Optional.empty()));
        }
    }

    @Test
    void interruptedResponseFailsScanAndReleasesConnection() throws Exception {
        server.createContext("/bucket/truncated.txt", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(200, 1024);
                exchange.getResponseBody().write('a');
                exchange.getResponseBody().flush();
            }
        });
        TextTableFunction.Handle handle = new TextTableFunction.Handle(
                new FileSelection(List.of(new S3ObjectRef("s3://bucket/truncated.txt", 1024, null, null, Compression.NONE)), ""),
                new ScanSettings(1024, new PageSettings(1024, 1024), "UTF-8"),
                new TextTableFunction.TextOptions("\n"));
        ConnectorPageSource source = new TextTableFunction(builder, 1024)
                .createPageSource(session, handle, FileSplit.forWholeFile(handle.selection().objects().getFirst()), List.of());

        try (source) {
            assertThrows(UncheckedIOException.class, source::getNextSourcePage);
            assertTrue(source.isFinished());
            assertEquals(null, source.getNextSourcePage());
        }
        try (var client = builder.forSession(session)) {
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, client.readBytes(
                    "s3://bucket/data.txt", 0, null, Optional.empty(), Optional.empty()));
        }
    }

    @Test
    void concurrentReadersOfSameQuerySurviveClosingOneReader() throws Exception {
        configureClient(Map.of("s3.max-connections-per-worker", "2"));
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch finishFirst = new CountDownLatch(1);
        CountDownLatch finishSecond = new CountDownLatch(1);
        holdResponse("/bucket/first.txt", started, finishFirst);
        holdResponse("/bucket/second.txt", started, finishSecond);
        CyclicBarrier start = new CyclicBarrier(2);
        var first = executor.submit(() -> {
            start.await(10, TimeUnit.SECONDS);
            try (var client = builder.forSession(session)) {
                return client.readBytes("s3://bucket/first.txt", 0, null, Optional.empty(), Optional.empty());
            }
        });
        var second = executor.submit(() -> {
            start.await(10, TimeUnit.SECONDS);
            try (var client = builder.forSession(session)) {
                return client.readBytes("s3://bucket/second.txt", 0, null, Optional.empty(), Optional.empty());
            }
        });

        try {
            assertTrue(started.await(10, TimeUnit.SECONDS), "both readers must have an active response");
            finishFirst.countDown();
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, first.get(10, TimeUnit.SECONDS));
            assertFalse(second.isDone());
            finishSecond.countDown();
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, second.get(10, TimeUnit.SECONDS));
            try (var reopened = builder.forSession(session)) {
                assertArrayEquals(new byte[] {'a', 'b', 'c'}, reopened.readBytes(
                        "s3://bucket/data.txt", 0, null, Optional.empty(), Optional.empty()));
            }
        }
        finally {
            finishFirst.countDown();
            finishSecond.countDown();
        }
    }

    @Test
    void concurrentQueriesKeepTheirOwnSessionInInterceptor() throws Exception {
        configureClient(Map.of("s3.interceptor-class", SessionHeaderInterceptor.class.getName()));
        when(session.getUser()).thenReturn("alice");
        ConnectorSession otherSession = mock(ConnectorSession.class);
        when(otherSession.getQueryId()).thenReturn("query-2");
        when(otherSession.getUser()).thenReturn("bob");
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch finish = new CountDownLatch(1);
        holdResponse("/bucket/context.txt", started, finish);
        var first = executor.submit(() -> {
            try (var client = builder.forSession(session)) {
                return client.readBytes("s3://bucket/context.txt", 0, null, Optional.empty(), Optional.empty());
            }
        });
        var second = executor.submit(() -> {
            try (var client = builder.forSession(otherSession)) {
                return client.readBytes("s3://bucket/context.txt", 0, null, Optional.empty(), Optional.empty());
            }
        });

        try {
            assertTrue(started.await(10, TimeUnit.SECONDS), "both queries must have an active response");
            assertEquals(Set.of(Map.of("query", "query-1", "user", "alice"),
                    Map.of("query", "query-2", "user", "bob")), Set.copyOf(requests));
            finish.countDown();
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, first.get(10, TimeUnit.SECONDS));
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, second.get(10, TimeUnit.SECONDS));
        }
        finally {
            finish.countDown();
        }
    }

    @Test
    void closingBuilderInterruptsActiveReadAndRejectsNewSessions() throws Exception {
        CountDownLatch finish = new CountDownLatch(1);
        CountDownLatch reading = new CountDownLatch(1);
        holdResponse("/bucket/blocked.txt", new CountDownLatch(0), finish);
        var read = executor.submit(() -> {
            try (var client = builder.forSession(session);
                    InputStream stream = client.openStream(
                            "s3://bucket/blocked.txt", 0, null, Optional.empty(), Optional.empty())) {
                assertEquals('a', stream.read());
                reading.countDown();
                return stream.readAllBytes();
            }
        });

        try {
            assertTrue(reading.await(10, TimeUnit.SECONDS), "reader must have consumed the start of the response");
            executor.submit(() -> builder.close()).get(10, TimeUnit.SECONDS);
            ExecutionException failure = assertThrows(ExecutionException.class, () -> read.get(10, TimeUnit.SECONDS));
            assertInstanceOf(IOException.class, failure.getCause());
            assertThrows(IllegalStateException.class, () -> builder.forSession(session));
        }
        finally {
            finish.countDown();
        }
    }

    private void configureClient(Map<String, String> overrides) {
        if (builder != null) {
            builder.close();
        }
        Map<String, String> config = new HashMap<>(Map.of(
                "s3.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                "s3.auth-mode", "anonymous",
                "s3.max-connections-per-worker", "1",
                "s3.connection-acquisition-timeout-s", "1"));
        config.putAll(overrides);
        builder = new S3ClientBuilder(S3ClientConfig.from(config));
    }

    private void holdResponse(String path, CountDownLatch started, CountDownLatch finish) {
        server.createContext(path, exchange -> {
            try (exchange) {
                requests.add(Map.of(
                        "query", Optional.ofNullable(exchange.getRequestHeaders().getFirst("X-Test-Query")).orElse(""),
                        "user", Optional.ofNullable(exchange.getRequestHeaders().getFirst("X-Test-User")).orElse("")));
                exchange.sendResponseHeaders(200, 3);
                exchange.getResponseBody().write('a');
                exchange.getResponseBody().flush();
                started.countDown();
                if (!finish.await(10, TimeUnit.SECONDS)) {
                    throw new IOException("test did not release response");
                }
                exchange.getResponseBody().write(new byte[] {'b', 'c'});
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException(e);
            }
        });
    }

    public static class SessionHeaderInterceptor implements ExecutionInterceptor {
        @Override
        public SdkHttpRequest modifyHttpRequest(Context.ModifyHttpRequest context, ExecutionAttributes attributes) {
            ConnectorSession session = attributes.getAttribute(S3ClientBuilder.SESSION_ATTRIBUTE);
            return context.httpRequest().toBuilder()
                    .putHeader("X-Test-Query", session.getQueryId())
                    .putHeader("X-Test-User", session.getUser())
                    .build();
        }
    }
}
