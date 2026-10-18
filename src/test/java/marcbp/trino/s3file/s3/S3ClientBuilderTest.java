package marcbp.trino.s3file.s3;

import com.sun.net.httpserver.HttpServer;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import marcbp.trino.s3file.file.PageSettings;
import marcbp.trino.s3file.file.S3ObjectRef;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.txt.TextTableFunction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class S3ClientBuilderTest {
    private final BlockingQueue<Map<String, String>> requests = new LinkedBlockingQueue<>();
    private HttpServer server;
    private S3ClientBuilder builder;
    private ConnectorSession session;

    @BeforeEach
    void setUp() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
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
        builder = new S3ClientBuilder(S3ClientConfig.from(Map.of(
                "s3.endpoint", "http://127.0.0.1:" + server.getAddress().getPort(),
                "s3.auth-mode", "anonymous",
                "s3.max-connections-per-worker", "1",
                "s3.connection-acquisition-timeout-s", "1")));
        session = mock(ConnectorSession.class);
        when(session.getQueryId()).thenReturn("query-1");
    }

    @AfterEach
    void tearDown() {
        if (builder != null) {
            builder.close();
        }
        if (server != null) {
            server.stop(0);
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

    @Test
    void closingOneSessionDoesNotBreakOtherReadersOfTheQuery() throws Exception {
        var first = builder.forSession(session);
        try (var second = builder.forSession(session)) {
            first.close();
            first.close();
            assertArrayEquals(new byte[] {'a', 'b', 'c'}, second.readBytes(
                    "s3://bucket/data.txt", 0, null, Optional.empty(), Optional.empty()));
        }
        try (var reopened = builder.forSession(session)) {
            assertEquals(3, reopened.readBytes(
                    "s3://bucket/data.txt", 0, null, Optional.empty(), Optional.empty()).length);
        }
        builder.close();
        assertThrows(IllegalStateException.class, () -> builder.forSession(session));
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
                new S3ObjectRef("s3://bucket/truncated.txt", 1024, null, null),
                new ScanSettings(1024, new PageSettings(1024, 1024), "UTF-8"),
                new TextTableFunction.TextOptions("\n"));
        ConnectorPageSource source = new TextTableFunction(builder, 1024)
                .createPageSource(session, handle, handle.toWholeFileSplit(), List.of());

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
}
