package marcbp.trino.s3file.util;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.type.BigintType;
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
    public static final String SPLIT_SIZE_MB_ARGUMENT = "SPLIT_SIZE_MB";
    private static final long USE_CONNECTOR_DEFAULT_SPLIT_SIZE_MB = -1L;

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

    public static ScalarArgumentSpecification splitSizeMbArgumentSpecification() {
        return ScalarArgumentSpecification.builder()
                .name(SPLIT_SIZE_MB_ARGUMENT)
                .type(BigintType.BIGINT)
                .defaultValue(USE_CONNECTOR_DEFAULT_SPLIT_SIZE_MB)
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

    public static int resolveSplitSizeBytes(Map<String, Argument> arguments, int defaultSplitSizeBytes) {
        ScalarArgument splitSizeArgument = (ScalarArgument) arguments.get(SPLIT_SIZE_MB_ARGUMENT);
        if (splitSizeArgument == null) {
            return defaultSplitSizeBytes;
        }

        Object rawValue = splitSizeArgument.getValue();
        if (!(rawValue instanceof Number splitSizeMbValue)) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SPLIT_SIZE_MB must be an integer");
        }

        long splitSizeMb = splitSizeMbValue.longValue();
        if (splitSizeMb == USE_CONNECTOR_DEFAULT_SPLIT_SIZE_MB) {
            return defaultSplitSizeBytes;
        }
        if (splitSizeMb <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SPLIT_SIZE_MB must be a positive integer");
        }

        long splitSizeBytes = splitSizeMb * 1024L * 1024L;
        if (splitSizeBytes > Integer.MAX_VALUE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SPLIT_SIZE_MB is too large");
        }
        return (int) splitSizeBytes;
    }
}
