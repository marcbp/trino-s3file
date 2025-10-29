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
import marcbp.trino.s3file.util.S3ClientBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

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

/**
 * Unit tests covering analysis and split creation for the text table function.
 */
class TextTableFunctionTest {
    private static final String PATH = "s3://bucket/messages.txt";

    private final S3ClientBuilder s3ClientBuilder = mock(S3ClientBuilder.class);
    private final S3ClientBuilder.SessionClient sessionClient = mock(S3ClientBuilder.SessionClient.class);
    private final TextTableFunction function = new TextTableFunction(s3ClientBuilder);

    @BeforeEach
    void setUp() throws IOException {
        reset(s3ClientBuilder, sessionClient);
        when(s3ClientBuilder.forSession(any(ConnectorSession.class))).thenReturn(sessionClient);
        doNothing().when(sessionClient).close();
    }

    @Test
    void analyzeBuildsHandleWithDecodedLineBreak() {
        when(sessionClient.getObjectSize(eq(PATH))).thenReturn(2048L);
        Map<String, Argument> arguments = Map.of(
                "PATH", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice(PATH)),
                "LINE_BREAK", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("\\r\\n"))
        );

        TableFunctionAnalysis analysis = function.analyze(
                mock(ConnectorSession.class),
                new ConnectorTransactionHandle() {},
                arguments,
                null);

        TextTableFunction.Handle handle = assertHandle(analysis, "\r\n", 2048L);
        assertEquals(Descriptor.descriptor(List.of("line"), List.of(VarcharType.createUnboundedVarcharType())),
                analysis.getReturnedType().orElseThrow());
        verify(sessionClient).getObjectSize(eq(PATH));
        verify(sessionClient).close();
    }

    @Test
    void createSplitsRespectsLookaheadAndBoundaries() {
        TextTableFunction.Handle handle = new TextTableFunction.Handle(
                PATH,
                "\n",
                null,
                10,
                4,
                StandardCharsets.UTF_8.name());

        List<ConnectorSplit> splits = function.createSplits(handle);

        assertEquals(3, splits.size());

        TextTableFunction.Split first = (TextTableFunction.Split) splits.get(0);
        assertEquals(0, first.getStartOffset());
        assertEquals(4, first.getPrimaryEndOffset());
        assertEquals(10, first.getRangeEndExclusive());
        assertTrue(first.isFirst());
        assertTrue(first.isLast());

        TextTableFunction.Split second = (TextTableFunction.Split) splits.get(1);
        assertEquals(4, second.getStartOffset());
        assertEquals(8, second.getPrimaryEndOffset());
        assertEquals(10, second.getRangeEndExclusive());
        assertFalse(second.isFirst());
        assertTrue(second.isLast());

        TextTableFunction.Split third = (TextTableFunction.Split) splits.get(2);
        assertEquals(8, third.getStartOffset());
        assertEquals(10, third.getPrimaryEndOffset());
        assertEquals(10, third.getRangeEndExclusive());
        assertFalse(third.isFirst());
        assertTrue(third.isLast());
    }

    private static TextTableFunction.Handle assertHandle(TableFunctionAnalysis analysis, String expectedDelimiter, long expectedSize) {
        assertInstanceOf(TextTableFunction.Handle.class, analysis.getHandle());
        TextTableFunction.Handle handle = (TextTableFunction.Handle) analysis.getHandle();
        assertEquals(PATH, handle.getS3Path());
        assertEquals(expectedDelimiter, handle.getLineBreak());
        assertEquals(expectedSize, handle.getFileSize());
        assertEquals(8 * 1024 * 1024, handle.getSplitSizeBytes());
        assertEquals(1024, handle.batchSizeOrDefault());
        assertEquals(StandardCharsets.UTF_8.name(), handle.getCharsetName());
        List<Type> columnTypes = handle.resolveColumnTypes();
        assertEquals(1, columnTypes.size());
        assertEquals(VarcharType.createUnboundedVarcharType(), columnTypes.get(0));
        return handle;
    }
}
