package marcbp.trino.s3file.csv;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3ObjectService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests that exercise the CSV table function's analysis workflow.
 */
class S3FileCsvTableFunctionTest {
    private static final String PATH = "s3://bucket/data.csv";

    private final S3ObjectService s3ObjectService = mock(S3ObjectService.class);
    private final CsvProcessingService csvProcessingService = new CsvProcessingService();
    private final S3FileCsvTableFunction function = new S3FileCsvTableFunction(s3ObjectService, csvProcessingService);

    @BeforeEach
    void setUp() {
        reset(s3ObjectService);
    }

    @Test
    void analyzeInfersColumnsFromHeader() {
        when(s3ObjectService.openReader(eq(PATH))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("first;second\n1;2\n")));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        S3FileCsvTableFunction.Handle handle = (S3FileCsvTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals(List.of("first", "second"), handle.getColumns());
        assertTrue(handle.isHeaderPresent());
        assertEquals(1024, handle.batchSizeOrDefault());

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("first", "second"),
                List.of(VarcharType.createUnboundedVarcharType(), VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        verify(s3ObjectService).openReader(PATH);
    }

    @Test
    void analyzeWithoutHeaderGeneratesDefaultColumns() {
        when(s3ObjectService.openReader(eq(PATH))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("value1;value2;value3\n1;2;3\n")));

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "HEADER", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("false"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        S3FileCsvTableFunction.Handle handle = (S3FileCsvTableFunction.Handle) analysis.getHandle();
        assertEquals(List.of("column_1", "column_2", "column_3"), handle.getColumns());
        assertTrue(handle.batchSizeOrDefault() > 0);

        Descriptor expectedDescriptor = Descriptor.descriptor(
                List.of("column_1", "column_2", "column_3"),
                List.of(
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType(),
                        VarcharType.createUnboundedVarcharType()));
        assertEquals(expectedDescriptor, analysis.getReturnedType().orElseThrow());

        verify(s3ObjectService).openReader(PATH);
    }
}
