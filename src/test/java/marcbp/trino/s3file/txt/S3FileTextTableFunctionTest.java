package marcbp.trino.s3file.txt;

import io.airlift.slice.Slices;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3ObjectService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests covering analysis and split creation for the text table function.
 */
class S3FileTextTableFunctionTest {
    private static final String PATH = "s3://bucket/messages.txt";

    private final S3ObjectService s3ObjectService = mock(S3ObjectService.class);
    private final S3FileTextTableFunction function = new S3FileTextTableFunction(s3ObjectService);

    @Test
    void analyzeBuildsHandleWithDecodedLineBreak() {
        when(s3ObjectService.getObjectSize(PATH)).thenReturn(2048L);
        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "LINE_BREAK", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("\\r\\n"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        S3FileTextTableFunction.Handle handle = assertHandle(analysis, "\r\n", 2048L);
        assertEquals(Descriptor.descriptor(List.of("line"), List.of(VarcharType.createUnboundedVarcharType())),
                analysis.getReturnedType().orElseThrow());
        verify(s3ObjectService).getObjectSize(PATH);
    }

    @Test
    void createSplitsRespectsLookaheadAndBoundaries() {
        S3FileTextTableFunction.Handle handle = new S3FileTextTableFunction.Handle(
                PATH,
                "\n",
                null,
                10,
                4);

        List<ConnectorSplit> splits = function.createSplits(handle);

        assertEquals(3, splits.size());

        S3FileTextTableFunction.Split first = (S3FileTextTableFunction.Split) splits.get(0);
        assertEquals(0, first.getStartOffset());
        assertEquals(4, first.getPrimaryEndOffset());
        assertEquals(10, first.getRangeEndExclusive());
        assertTrue(first.isFirst());
        assertTrue(first.isLast());

        S3FileTextTableFunction.Split second = (S3FileTextTableFunction.Split) splits.get(1);
        assertEquals(4, second.getStartOffset());
        assertEquals(8, second.getPrimaryEndOffset());
        assertEquals(10, second.getRangeEndExclusive());
        assertFalse(second.isFirst());
        assertTrue(second.isLast());

        S3FileTextTableFunction.Split third = (S3FileTextTableFunction.Split) splits.get(2);
        assertEquals(8, third.getStartOffset());
        assertEquals(10, third.getPrimaryEndOffset());
        assertEquals(10, third.getRangeEndExclusive());
        assertFalse(third.isFirst());
        assertTrue(third.isLast());
    }

    private static S3FileTextTableFunction.Handle assertHandle(TableFunctionAnalysis analysis, String expectedDelimiter, long expectedSize) {
        assertInstanceOf(S3FileTextTableFunction.Handle.class, analysis.getHandle());
        S3FileTextTableFunction.Handle handle = (S3FileTextTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals(expectedDelimiter, handle.getLineBreak());
        assertEquals(expectedSize, handle.getFileSize());
        assertEquals(8 * 1024 * 1024, handle.getSplitSizeBytes());
        assertEquals(1024, handle.batchSizeOrDefault());
        List<Type> columnTypes = handle.resolveColumnTypes();
        assertEquals(1, columnTypes.size());
        assertEquals(VarcharType.createUnboundedVarcharType(), columnTypes.get(0));
        return handle;
    }
}
