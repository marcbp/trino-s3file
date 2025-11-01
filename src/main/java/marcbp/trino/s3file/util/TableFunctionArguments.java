package marcbp.trino.s3file.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.type.VarcharType;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/**
 * Shared helper for common S3-backed table function arguments.
 */
public final class TableFunctionArguments {
    public static final String PATH_ARGUMENT = "PATH";
    public static final String ENCODING_ARGUMENT = "ENCODING";

    private TableFunctionArguments() {}

    public static ScalarArgumentSpecification pathArgumentSpecification() {
        return ScalarArgumentSpecification.builder()
                .name(PATH_ARGUMENT)
                .type(VarcharType.VARCHAR)
                .build();
    }

    public static ScalarArgumentSpecification encodingArgumentSpecification() {
        return ScalarArgumentSpecification.builder()
                .name(ENCODING_ARGUMENT)
                .type(VarcharType.VARCHAR)
                .defaultValue(Slices.utf8Slice(StandardCharsets.UTF_8.name()))
                .build();
    }

    public static String requirePath(Map<String, Argument> arguments) {
        ScalarArgument pathArgument = (ScalarArgument) arguments.get(PATH_ARGUMENT);
        if (pathArgument == null) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Argument PATH is required");
        }
        Object rawValue = pathArgument.getValue();
        if (!(rawValue instanceof Slice slice)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PATH must be a string");
        }
        String s3Path = slice.toStringUtf8();
        if (s3Path.isBlank()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "PATH cannot be blank");
        }
        return s3Path;
    }

    public static Charset resolveEncoding(Map<String, Argument> arguments) {
        return CharsetUtils.resolve(arguments, ENCODING_ARGUMENT);
    }
}
