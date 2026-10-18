package marcbp.trino.s3file.s3;

import com.sun.net.httpserver.HttpServer;
import io.trino.spi.connector.ConnectorSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
                "s3.auth-mode", "anonymous")));
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
}
