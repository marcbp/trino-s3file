package marcbp.trino.s3file.json;

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
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AnalysisStats;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.S3ObjectRef;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3ClientBuilder.ObjectMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class JsonTableFunctionTest {
    private static final String PATH = "s3://bucket/events.jsonl";
    private static final int CONNECTOR_SPLIT_SIZE_BYTES = 16 * 1024 * 1024;

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final JsonTableFunction function = new JsonTableFunction(s3ClientBuilder, CONNECTOR_SPLIT_SIZE_BYTES);

    @BeforeEach
    void setUp() {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeInfersTypesFromSampleRows() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1,"active":true,"amount":12.5,"meta":{"source":"app"}}
                        {"event_id":2,"active":false,"amount":7.0,"note":"bye"}
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(512L, Optional.of("etag-json"), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("event_id", "active", "amount", "meta", "note"),
                List.of(
                        BigintType.BIGINT,
                        BooleanType.BOOLEAN,
                        DoubleType.DOUBLE,
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, descriptor);

        JsonTableFunction.Handle handle = (JsonTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.object().path());
        assertEquals(expectedDescriptor, Descriptor.descriptor(handle.schema().columns(), handle.resolveColumnTypes()));
        assertEquals(512L, handle.object().size());
        assertEquals(CONNECTOR_SPLIT_SIZE_BYTES, handle.scan().splitSizeBytes());
        assertEquals(Optional.of("etag-json"), handle.object().eTagRef());
        assertEquals(Optional.empty(), handle.object().versionIdRef());
        assertEquals(StandardCharsets.UTF_8.name(), handle.scan().charsetName());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeHonorsSchemaSampleRowLimit() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1,"active":true}
                        {"event_id":2,"active":false,"note":"later"}
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(128L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "SCHEMA_SAMPLE_ROWS", new ScalarArgument(BigintType.BIGINT, 1L)
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("event_id", "active"),
                List.of(BigintType.BIGINT, BooleanType.BOOLEAN));
        assertEquals(expectedDescriptor, descriptor);

        verify(sessionClient).close();
    }

    @Test
    void analyzeAllowsSplitSizeOverridePerRequest() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1}
                        {"event_id":2}
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(128L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "SPLIT_SIZE_MB", new ScalarArgument(io.trino.spi.type.BigintType.BIGINT, 2L)
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        JsonTableFunction.Handle handle = (JsonTableFunction.Handle) analysis.getHandle();
        assertEquals(2 * 1024 * 1024, handle.scan().splitSizeBytes());
    }

    @Test
    void analyzeAppliesAdditionalColumns() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1}
                        {"event_id":2,"campaign":"spring","score":42.5}
                        """)));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(256L, Optional.empty(), Optional.of("v2")));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "ADDITIONAL_COLUMNS", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("campaign:varchar,score:double")),
                "ENCODING", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("ISO-8859-1"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        Descriptor descriptor = analysis.getReturnedType().orElseThrow();
        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("event_id", "campaign", "score"),
                List.of(
                        BigintType.BIGINT,
                        VarcharType.createUnboundedVarcharType(),
                        DoubleType.DOUBLE));
        assertEquals(expectedDescriptor, descriptor);

        JsonTableFunction.Handle handle = (JsonTableFunction.Handle) analysis.getHandle();
        assertEquals(List.of("event_id", "campaign", "score"), handle.schema().columns());
        assertEquals(
                List.of(BigintType.BIGINT, VarcharType.createUnboundedVarcharType(), DoubleType.DOUBLE),
                handle.resolveColumnTypes());
        assertEquals("ISO-8859-1", handle.scan().charsetName());
        assertEquals(Optional.empty(), handle.object().eTagRef());
        assertEquals(Optional.of("v2"), handle.object().versionIdRef());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void pageSourceSkipsPartialFirstLineOnNonInitialSplit() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(11L), eq(12L), any(), any())).thenReturn(new byte[] {'x'});
        when(sessionClient.openReader(eq(PATH), eq(12L), eq(64L), any(Charset.class), any(), any())).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        ive":false}
                        {"event_id":2,"active":true}
                        """)));

        JsonTableFunction.Handle handle = handle(
                List.of("event_id", "active"),
                List.of(JsonFormatSupport.ColumnType.BIGINT, JsonFormatSupport.ColumnType.BOOLEAN),
                256,
                64);
        FileSplit split = new FileSplit("split-1", 12, 32, 64, false, false);

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, split, allColumns(handle));
        SourcePage page = nextPage(pageSource);

        assertEquals(1, page.getPositionCount());
        assertEquals(2L, BigintType.BIGINT.getObjectValue(page.getBlock(0), 0));
        assertEquals(true, BooleanType.BOOLEAN.getObjectValue(page.getBlock(1), 0));
        assertEquals(null, pageSource.getNextSourcePage());

        verify(sessionClient).openReader(eq(PATH), eq(12L), eq(64L), any(Charset.class), any(), any());
    }

    @Test
    void pageSourceKeepsFirstLineWhenSplitAlreadyStartsAtBoundary() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(31L), eq(32L), any(), any())).thenReturn(new byte[] {'\n'});
        when(sessionClient.openReader(eq(PATH), eq(32L), eq(96L), any(Charset.class), any(), any())).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":2,"active":true}
                        {"event_id":3,"active":false}
                        """)));

        JsonTableFunction.Handle handle = handle(
                List.of("event_id", "active"),
                List.of(JsonFormatSupport.ColumnType.BIGINT, JsonFormatSupport.ColumnType.BOOLEAN),
                256,
                64);
        FileSplit split = new FileSplit("split-1", 32, 64, 96, false, false);

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, split, allColumns(handle));
        SourcePage page = nextPage(pageSource);

        assertEquals(2, page.getPositionCount());
        assertEquals(2L, BigintType.BIGINT.getObjectValue(page.getBlock(0), 0));
        assertEquals(3L, BigintType.BIGINT.getObjectValue(page.getBlock(0), 1));
        assertEquals(null, pageSource.getNextSourcePage());
    }

    private static List<S3FileColumnHandle> allColumns(JsonTableFunction.Handle handle) {
        return java.util.stream.IntStream.range(0, handle.schema().columns().size())
                .mapToObj(index -> new S3FileColumnHandle(handle.schema().columns().get(index), index))
                .toList();
    }

    private static JsonTableFunction.Handle handle(List<String> columns, List<JsonFormatSupport.ColumnType> columnTypes, long fileSize, int splitSizeBytes) {
        return new JsonTableFunction.Handle(
                new S3ObjectRef(PATH, fileSize, null, null),
                new ScanSettings(splitSizeBytes, JsonTableFunction.Handle.DEFAULT_BATCH_SIZE, StandardCharsets.UTF_8.name()),
                AnalysisStats.EMPTY,
                new JsonTableFunction.JsonSchema(columns, columnTypes));
    }

    private static SourcePage nextPage(ConnectorPageSource pageSource) {
        SourcePage page = pageSource.getNextSourcePage();
        assertInstanceOf(SourcePage.class, page);
        return page;
    }
}
