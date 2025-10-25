package marcbp.trino.s3file.json;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3ObjectService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3FileJsonTableFunctionTest {
    private static final String PATH = "s3://bucket/events.jsonl";

    private final S3ObjectService s3ObjectService = mock(S3ObjectService.class);
    private final S3FileJsonTableFunction function = new S3FileJsonTableFunction(s3ObjectService);

    @BeforeEach
    void setUp() {
        reset(s3ObjectService);
    }

    @Test
    void analyzeInfersTypesFromFirstDocument() {
        when(s3ObjectService.openReader(eq(PATH))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1,"active":true,"amount":12.5,"meta":{"source":"app"},"note":"hello"}
                        {"event_id":2,"active":false,"amount":7.0,"note":"bye"}
                        """)));
        when(s3ObjectService.getObjectSize(PATH)).thenReturn(512L);

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

        S3FileJsonTableFunction.Handle handle = (S3FileJsonTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals(expectedDescriptor, Descriptor.descriptor(handle.getColumns(), handle.resolveColumnTypes()));
        assertEquals(512L, handle.getFileSize());

        verify(s3ObjectService).openReader(PATH);
        verify(s3ObjectService).getObjectSize(PATH);
    }

    @Test
    void analyzeAppliesAdditionalColumns() {
        when(s3ObjectService.openReader(eq(PATH))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1}
                        {"event_id":2,"campaign":"spring","score":42.5}
                        """)));
        when(s3ObjectService.getObjectSize(PATH)).thenReturn(256L);

        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "ADDITIONAL_COLUMNS", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("campaign:varchar,score:double"))
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

        S3FileJsonTableFunction.Handle handle = (S3FileJsonTableFunction.Handle) analysis.getHandle();
        assertEquals(List.of("event_id", "campaign", "score"), handle.getColumns());
        assertEquals(
                List.of(BigintType.BIGINT, VarcharType.createUnboundedVarcharType(), DoubleType.DOUBLE),
                handle.resolveColumnTypes());

        verify(s3ObjectService).openReader(PATH);
        verify(s3ObjectService).getObjectSize(PATH);
    }
}
