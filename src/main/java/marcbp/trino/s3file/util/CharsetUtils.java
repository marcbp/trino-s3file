package marcbp.trino.s3file.util;

import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Shared helpers for parsing charset configuration from table-function arguments.
 */
public final class CharsetUtils {
    private CharsetUtils() {}

    public static Charset resolve(Map<String, Argument> arguments, String argumentName) {
        Argument argument = arguments.get(argumentName);
        if (!(argument instanceof ScalarArgument scalarArgument)) {
            return StandardCharsets.UTF_8;
        }
        Object value = scalarArgument.getValue();
        if (!(value instanceof Slice slice)) {
            return StandardCharsets.UTF_8;
        }
        String name = slice.toStringUtf8().trim();
        return resolve(name);
    }

    public static Charset resolve(String charsetName) {
        if (charsetName == null) {
            return StandardCharsets.UTF_8;
        }
        String name = charsetName.trim();
        if (name.isEmpty()) {
            return StandardCharsets.UTF_8;
        }
        try {
            return Charset.forName(name);
        }
        catch (IllegalArgumentException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported encoding: " + charsetName, e);
        }
    }
}
