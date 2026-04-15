package marcbp.trino.s3file.csv;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests that exercise the CSV table function's analysis workflow.
 */
class CsvTableFunctionTest {
    private static final String PATH = "s3://bucket/data.csv";
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 32 * 1024 * 1024;

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final CsvTableFunction function = new CsvTableFunction(s3ClientBuilder, DEFAULT_SPLIT_SIZE_BYTES);

    @BeforeEach
    void setUp() {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeInfersColumnsFromHeader() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("first;second\n1;2\n")));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(64L, Optional.of("etag-1"), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        CsvTableFunction.Handle handle = (CsvTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.object().path());
        assertEquals(List.of("first", "second"), handle.schema().columns());
        assertTrue(handle.options().headerPresent());
        assertEquals(1024, handle.scan().batchSize());
        assertEquals(64L, handle.object().size());
        assertEquals(Optional.of("etag-1"), handle.object().eTagRef());
        assertEquals(Optional.empty(), handle.object().versionIdRef());
        assertEquals(DEFAULT_SPLIT_SIZE_BYTES, handle.scan().splitSizeBytes());
        assertEquals(StandardCharsets.UTF_8.name(), handle.scan().charsetName());

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("first", "second"),
                List.of(VarcharType.createUnboundedVarcharType(), VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeWithoutHeaderGeneratesDefaultColumns() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("value1;value2;value3\n1;2;3\n")));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(64L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "HEADER", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("false"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        CsvTableFunction.Handle handle = (CsvTableFunction.Handle) analysis.getHandle();
        assertEquals(List.of("column_1", "column_2", "column_3"), handle.schema().columns());
        assertTrue(handle.scan().batchSize() > 0);
        assertEquals(64L, handle.object().size());
        assertEquals(StandardCharsets.UTF_8.name(), handle.scan().charsetName());

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("column_1", "column_2", "column_3"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectMetadata(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeAllowsSplitSizeOverridePerRequest() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("first;second\n1;2\n")));
        when(sessionClient.getObjectMetadata(eq(PATH))).thenReturn(new ObjectMetadata(64L, Optional.empty(), Optional.empty()));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "SPLIT_SIZE_MB", new ScalarArgument(io.trino.spi.type.BigintType.BIGINT, 4L)
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        CsvTableFunction.Handle handle = (CsvTableFunction.Handle) analysis.getHandle();
        assertEquals(4 * 1024 * 1024, handle.scan().splitSizeBytes());
    }

    @Test
    void createSplitsGeneratesMultipleRanges() {
        CsvTableFunction.Handle handle = handle(List.of("c1", "c2"), true, 25 * 1024 * 1024L, 8 * 1024 * 1024);

        List<FileSplit> splits = function.createSplits(handle);

        assertEquals(4, splits.size());
        FileSplit first = splits.get(0);
        assertEquals(0, first.getStartOffset());
        assertTrue(first.isFirst());
        assertFalse(first.isLast());
        FileSplit last = splits.get(splits.size() - 1);
        assertTrue(last.isLast());
        assertTrue(last.getStartOffset() < handle.object().size());
    }

    @Test
    void pageSourceKeepsBoundaryAlignedFirstRowOnNonInitialSplit() throws IOException {
        when(sessionClient.readBytes(eq(PATH), eq(9L), eq(10L), any(), any())).thenReturn(new byte[] {'\n'});
        when(sessionClient.openReader(eq(PATH), eq(10L), eq(40L), any(Charset.class), any(), any())).thenAnswer(invocation ->
                new BufferedReader(new StringReader("3;4\n5;6\n")));

        CsvTableFunction.Handle handle = handle(List.of("c1", "c2"), false, 128, 32);
        FileSplit split = new FileSplit("split-1", 10, 20, 40, false, false);

        ConnectorPageSource pageSource = function.createPageSource(mock(ConnectorSession.class), handle, split, allColumns(handle));
        SourcePage page = nextPage(pageSource);

        assertEquals(2, page.getPositionCount());
        assertEquals("3", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 0));
        assertEquals("5", VarcharType.createUnboundedVarcharType().getObjectValue(page.getBlock(0), 1));
        assertEquals(null, pageSource.getNextSourcePage());
    }

    private static List<S3FileColumnHandle> allColumns(CsvTableFunction.Handle handle) {
        return java.util.stream.IntStream.range(0, handle.schema().columns().size())
                .mapToObj(index -> new S3FileColumnHandle(handle.schema().columns().get(index), index))
                .toList();
    }

    private static CsvTableFunction.Handle handle(List<String> columns, boolean headerPresent, long fileSize, int splitSizeBytes) {
        return new CsvTableFunction.Handle(
                new S3ObjectRef(PATH, fileSize, null, null),
                new ScanSettings(splitSizeBytes, CsvTableFunction.Handle.DEFAULT_BATCH_SIZE, StandardCharsets.UTF_8.name()),
                AnalysisStats.EMPTY,
                new CsvTableFunction.CsvSchema(columns),
                new CsvTableFunction.CsvOptions(';', headerPresent));
    }

    private static SourcePage nextPage(ConnectorPageSource pageSource) {
        SourcePage page = pageSource.getNextSourcePage();
        assertInstanceOf(SourcePage.class, page);
        return page;
    }
}
