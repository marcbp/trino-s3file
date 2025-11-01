package marcbp.trino.s3file.csv;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.util.S3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import marcbp.trino.s3file.file.FileSplit;

import static org.mockito.Mockito.*;

/**
 * Unit tests that exercise the CSV table function's analysis workflow.
 */
class CsvTableFunctionTest {
    private static final String PATH = "s3://bucket/data.csv";

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final CsvTableFunction function = new CsvTableFunction(s3ClientBuilder);

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
        when(sessionClient.getObjectSize(eq(PATH))).thenReturn(64L);

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        CsvTableFunction.Handle handle = (CsvTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals(List.of("first", "second"), handle.getColumns());
        assertTrue(handle.isHeaderPresent());
        assertEquals(1024, handle.batchSizeOrDefault());
        assertEquals(64L, handle.getFileSize());
        assertEquals(8 * 1024 * 1024, handle.getSplitSizeBytes());
        assertEquals(StandardCharsets.UTF_8.name(), handle.getCharsetName());

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("first", "second"),
                List.of(VarcharType.createUnboundedVarcharType(), VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectSize(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void analyzeWithoutHeaderGeneratesDefaultColumns() {
        when(sessionClient.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("value1;value2;value3\n1;2;3\n")));
        when(sessionClient.getObjectSize(eq(PATH))).thenReturn(64L);

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
        assertEquals(List.of("column_1", "column_2", "column_3"), handle.getColumns());
        assertTrue(handle.batchSizeOrDefault() > 0);
        assertEquals(64L, handle.getFileSize());
        assertEquals(StandardCharsets.UTF_8.name(), handle.getCharsetName());

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("column_1", "column_2", "column_3"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        verify(sessionClient).openReader(eq(PATH), any(Charset.class));
        verify(sessionClient).getObjectSize(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void createSplitsGeneratesMultipleRanges() {
        CsvTableFunction.Handle handle = new CsvTableFunction.Handle(
                PATH,
                List.of("c1", "c2"),
                ';',
                true,
                null,
                25 * 1024 * 1024L,
                8 * 1024 * 1024,
                StandardCharsets.UTF_8.name());

        List<FileSplit> splits = function.createSplits(handle);

        assertEquals(4, splits.size());
        FileSplit first = splits.get(0);
        assertEquals(0, first.getStartOffset());
        assertTrue(first.isFirst());
        assertFalse(first.isLast());
        FileSplit last = splits.get(splits.size() - 1);
        assertTrue(last.isLast());
        assertTrue(last.getStartOffset() < handle.getFileSize());
    }
}
