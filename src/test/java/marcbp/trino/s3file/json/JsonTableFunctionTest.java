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
import marcbp.trino.s3file.util.S3ObjectService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JsonTableFunctionTest {
    private static final String PATH = "s3://bucket/events.jsonl";

    private final S3ObjectService s3ObjectService = mock(S3ObjectService.class);
    private final JsonTableFunction function = new JsonTableFunction(s3ObjectService);

    @BeforeEach
    void setUp() {
        reset(s3ObjectService);
    }

    @Test
    void analyzeInfersTypesFromFirstDocument() {
        when(s3ObjectService.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
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

        JsonTableFunction.Handle handle = (JsonTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals(expectedDescriptor, Descriptor.descriptor(handle.getColumns(), handle.resolveColumnTypes()));
        assertEquals(512L, handle.getFileSize());
        assertEquals(StandardCharsets.UTF_8.name(), handle.getCharsetName());

        verify(s3ObjectService).openReader(eq(PATH), any(Charset.class));
        verify(s3ObjectService).getObjectSize(PATH);
    }

    @Test
    void analyzeAppliesAdditionalColumns() {
        when(s3ObjectService.openReader(eq(PATH), any(Charset.class))).thenAnswer(invocation ->
                new BufferedReader(new StringReader("""
                        {"event_id":1}
                        {"event_id":2,"campaign":"spring","score":42.5}
                        """)));
        when(s3ObjectService.getObjectSize(PATH)).thenReturn(256L);

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

        verify(s3ObjectService).openReader(eq(PATH), any(Charset.class));
        verify(s3ObjectService).getObjectSize(PATH);
    }
}
