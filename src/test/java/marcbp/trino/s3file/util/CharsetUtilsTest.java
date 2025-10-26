package marcbp.trino.s3file.util;

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CharsetUtilsTest {

    @Test
    void resolveMapReturnsUtf8WhenArgumentMissing() {
        Charset charset = CharsetUtils.resolve(Map.of(), "ENCODING");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    void resolveMapReturnsTrimmedCharset() {
        Map<String, Argument> arguments = Map.of(
                "ENCODING", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("  ISO-8859-1  "))
        );

        Charset charset = CharsetUtils.resolve(arguments, "ENCODING");
        assertEquals(StandardCharsets.ISO_8859_1, charset);
    }

    @Test
    void resolveMapFallsBackWhenSliceBlank() {
        Map<String, Argument> arguments = Map.of(
                "ENCODING", new ScalarArgument(VarcharType.VARCHAR, Slices.utf8Slice("   "))
        );

        Charset charset = CharsetUtils.resolve(arguments, "ENCODING");
        assertEquals(StandardCharsets.UTF_8, charset);
    }

    @Test
    void resolveStringThrowsForUnsupportedCharset() {
        TrinoException exception = assertThrows(
                TrinoException.class,
                () -> CharsetUtils.resolve("invalid-charset"));

        assertEquals(INVALID_FUNCTION_ARGUMENT.toErrorCode(), exception.getErrorCode());
    }
}
