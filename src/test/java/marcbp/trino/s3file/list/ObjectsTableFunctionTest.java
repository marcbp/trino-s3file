package marcbp.trino.s3file.list;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3ClientBuilder.ListObjectsPage;
import marcbp.trino.s3file.s3.S3ClientBuilder.ListedObject;
import marcbp.trino.s3file.s3.S3ClientBuilder.ListedPrefix;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObjectsTableFunctionTest {
    private static final String BUCKET = "bucket";
    private static final String PREFIX = "data/";

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final ObjectsTableFunction function = new ObjectsTableFunction(s3ClientBuilder);

    @BeforeEach
    void setUp() {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeBuildsObjectListingDescriptor() {
        Map<String, Argument> arguments = Map.of(
                "BUCKET", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(BUCKET)),
                "PREFIX", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PREFIX)),
                "RECURSIVE_LISTING", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("false")),
                "INCLUDE_PREFIXES", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("true"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("path", "bucket", "key", "name", "parent", "size", "last_modified", "etag", "type"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        BigintType.BIGINT,
                        TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS,
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        ObjectsTableFunction.Handle handle = (ObjectsTableFunction.Handle) analysis.getHandle();
        assertEquals(BUCKET, handle.bucket());
        assertEquals(PREFIX, handle.prefix());
        assertFalse(handle.recursive());
        assertTrue(handle.includePrefixes());
        assertEquals(List.of("path", "bucket", "key", "name", "parent", "size", "last_modified", "etag", "type"), handle.columnNames());
        assertEquals(9, handle.resolveColumnTypes().size());
        assertEquals(0L, handle.analysis().rowsSampled());
    }

    @Test
    void pageSourceStreamsObjectsAndPrefixesAcrossS3Pages() {
        when(sessionClient.listObjects(eq("bucket"), eq("data/"), eq(false), any()))
                .thenReturn(new ListObjectsPage(
                        List.of(new ListedObject(
                                "bucket",
                                "data/alpha.txt",
                                12L,
                                Optional.of(Instant.parse("2026-01-02T03:04:05Z")),
                                Optional.of("\"etag-1\""))),
                        List.of(new ListedPrefix("bucket", "data/nested/")),
                        Optional.of("token-1")))
                .thenReturn(new ListObjectsPage(
                        List.of(new ListedObject(
                                "bucket",
                                "data/beta.txt",
                                34L,
                                Optional.empty(),
                                Optional.of("\"etag-2\""))),
                        List.of(),
                        Optional.empty()));

        ObjectsTableFunction.Handle handle = (ObjectsTableFunction.Handle) function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                Map.of(
                        "BUCKET", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(BUCKET)),
                        "PREFIX", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PREFIX)),
                        "RECURSIVE_LISTING", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("false")),
                        "INCLUDE_PREFIXES", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("true"))
                ),
                null).getHandle();

        List<S3FileColumnHandle> columns = List.of(
                new S3FileColumnHandle("path", 0),
                new S3FileColumnHandle("bucket", 1),
                new S3FileColumnHandle("key", 2),
                new S3FileColumnHandle("name", 3),
                new S3FileColumnHandle("parent", 4),
                new S3FileColumnHandle("size", 5),
                new S3FileColumnHandle("last_modified", 6),
                new S3FileColumnHandle("etag", 7),
                new S3FileColumnHandle("type", 8));

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, ListingSplit.INSTANCE, columns);
        SourcePage page = nextPage(pageSource);

        assertEquals(3, page.getPositionCount());
        assertEquals("s3://bucket/data/alpha.txt", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 0));
        assertEquals("bucket", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(1), 0));
        assertEquals("data/alpha.txt", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(2), 0));
        assertEquals("alpha.txt", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(3), 0));
        assertFalse(page.getBlock(6).isNull(0));
        assertEquals("prefix", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(8), 1));
        assertTrue(page.getBlock(5).isNull(1));
        assertEquals("object", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(8), 2));
        assertEquals(null, pageSource.getNextSourcePage());

        verify(sessionClient).listObjects(eq("bucket"), eq("data/"), eq(false), eq(Optional.<String>empty()));
        verify(sessionClient).listObjects(eq("bucket"), eq("data/"), eq(false), eq(Optional.of("token-1")));
        verify(sessionClient).close();
    }

    private static SourcePage nextPage(ConnectorPageSource pageSource) {
        SourcePage page = pageSource.getNextSourcePage();
        assertInstanceOf(SourcePage.class, page);
        return page;
    }
}
