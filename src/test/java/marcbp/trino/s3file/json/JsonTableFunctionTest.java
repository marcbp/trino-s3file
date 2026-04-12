package marcbp.trino.s3file.json;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.file.FileSplit;
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
        assertEquals(PATH, handle.getS3Path());
        assertEquals(expectedDescriptor, Descriptor.descriptor(handle.getColumns(), handle.resolveColumnTypes()));
        assertEquals(512L, handle.getFileSize());
        assertEquals(CONNECTOR_SPLIT_SIZE_BYTES, handle.getSplitSizeBytes());
        assertEquals(Optional.of("etag-json"), handle.getETag());
        assertEquals(Optional.empty(), handle.getVersionId());
        assertEquals(StandardCharsets.UTF_8.name(), handle.getCharsetName());

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
        assertEquals(2 * 1024 * 1024, handle.getSplitSizeBytes());
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
        assertEquals(List.of("event_id", "campaign", "score"), handle.getColumns());
        assertEquals(
                List.of(BigintType.BIGINT, VarcharType.createUnboundedVarcharType(), DoubleType.DOUBLE),
                handle.resolveColumnTypes());
        assertEquals("ISO-8859-1", handle.getCharsetName());
        assertEquals(Optional.empty(), handle.getETag());
        assertEquals(Optional.of("v2"), handle.getVersionId());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void processorSkipsPartialFirstLineOnNonInitialSplit() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(11L), eq(12L), any(), any())).thenReturn(new byte[] {'x'});
        when(sessionClient.openReader(eq(PATH), eq(12L), eq(64L), any(Charset.class), any(), any())).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        ive":false}
                        {"event_id":2,"active":true}
                        """)));

        JsonTableFunction.Handle handle = new JsonTableFunction.Handle(
                PATH,
                List.of("event_id", "active"),
                List.of(JsonFormatSupport.ColumnType.BIGINT, JsonFormatSupport.ColumnType.BOOLEAN),
                null,
                256,
                64,
                StandardCharsets.UTF_8.name(),
                null,
                null);
        FileSplit split = new FileSplit("split-1", 12, 32, 64, false, false);

        TableFunctionSplitProcessor processor = function.createSplitProcessor(mock(ConnectorSession.class), handle, split);
        TableFunctionProcessorState state = processor.process();

        TableFunctionProcessorState.Processed produced = assertInstanceOf(TableFunctionProcessorState.Processed.class, state);
        assertEquals(1, produced.getResult().getPositionCount());
        assertEquals(2L, BigintType.BIGINT.getObjectValue(produced.getResult().getBlock(0), 0));
        assertEquals(true, BooleanType.BOOLEAN.getObjectValue(produced.getResult().getBlock(1), 0));
        assertEquals(TableFunctionProcessorState.Finished.FINISHED, processor.process());

        verify(sessionClient).openReader(eq(PATH), eq(12L), eq(64L), any(Charset.class), any(), any());
    }

    @Test
    void processorKeepsFirstLineWhenSplitAlreadyStartsAtBoundary() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(31L), eq(32L), any(), any())).thenReturn(new byte[] {'\n'});
        when(sessionClient.openReader(eq(PATH), eq(32L), eq(96L), any(Charset.class), any(), any())).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":2,"active":true}
                        {"event_id":3,"active":false}
                        """)));

        JsonTableFunction.Handle handle = new JsonTableFunction.Handle(
                PATH,
                List.of("event_id", "active"),
                List.of(JsonFormatSupport.ColumnType.BIGINT, JsonFormatSupport.ColumnType.BOOLEAN),
                null,
                256,
                64,
                StandardCharsets.UTF_8.name(),
                null,
                null);
        FileSplit split = new FileSplit("split-1", 32, 64, 96, false, false);

        TableFunctionSplitProcessor processor = function.createSplitProcessor(mock(ConnectorSession.class), handle, split);
        TableFunctionProcessorState state = processor.process();

        TableFunctionProcessorState.Processed produced = assertInstanceOf(TableFunctionProcessorState.Processed.class, state);
        assertEquals(2, produced.getResult().getPositionCount());
        assertEquals(2L, BigintType.BIGINT.getObjectValue(produced.getResult().getBlock(0), 0));
        assertEquals(3L, BigintType.BIGINT.getObjectValue(produced.getResult().getBlock(0), 1));
    }
}
