package marcbp.trino.s3file.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Utilities encapsulating JSON-specific parsing and column inference logic.
 */
public final class JsonFormatSupport {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private JsonFormatSupport() {
    }

    public static ColumnsMetadata inferColumns(BufferedReader reader, String path) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            ObjectNode objectNode = parseObject(line, path);
            List<String> names = new ArrayList<>();
            List<ColumnType> types = new ArrayList<>();
            objectNode.fieldNames().forEachRemaining(field -> {
                names.add(field);
                types.add(inferColumnType(objectNode.get(field)));
            });
            if (!names.isEmpty()) {
                return new ColumnsMetadata(names, types);
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "No JSON object found in " + path);
    }

    public static List<ColumnDefinition> parseAdditionalColumns(Map<String, Argument> arguments) {
        Argument argument = arguments.get(JsonTableFunction.ADDITIONAL_COLUMNS_ARGUMENT);
        if (!(argument instanceof ScalarArgument scalarArgument)) {
            return List.of();
        }
        Object value = scalarArgument.getValue();
        if (!(value instanceof Slice slice)) {
            return List.of();
        }
        String specification = slice.toStringUtf8().trim();
        if (specification.isEmpty()) {
            return List.of();
        }
        List<ColumnDefinition> result = new ArrayList<>();
        String[] tokens = specification.split(",");
        for (String token : tokens) {
            String entry = token.trim();
            if (entry.isEmpty()) {
                continue;
            }
            int separator = entry.indexOf(':');
            if (separator <= 0 || separator == entry.length() - 1) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid additional column specification: " + entry);
            }
            String name = entry.substring(0, separator).trim();
            String typeToken = entry.substring(separator + 1).trim();
            if (name.isEmpty() || typeToken.isEmpty()) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid additional column specification: " + entry);
            }
            ColumnType columnType = ColumnType.fromSpecification(typeToken);
            result.add(new ColumnDefinition(name, columnType));
        }
        return List.copyOf(result);
    }

    public static void mergeAdditionalColumns(List<String> names, List<ColumnType> types, List<ColumnDefinition> additionalColumns) {
        for (ColumnDefinition definition : additionalColumns) {
            int existingIndex = names.indexOf(definition.name());
            if (existingIndex >= 0) {
                types.set(existingIndex, definition.type());
            }
            else {
                names.add(definition.name());
                types.add(definition.type());
            }
        }
    }

    public static List<String> describeColumns(List<String> names, List<ColumnType> types) {
        List<String> summary = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            summary.add(names.get(i) + ":" + types.get(i).name().toLowerCase(Locale.ROOT));
        }
        return summary;
    }

    public static ObjectNode parseObject(String json, String path) {
        try {
            JsonNode node = OBJECT_MAPPER.readTree(json);
            if (!(node instanceof ObjectNode objectNode)) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "JSON row must be an object in " + path);
            }
            return objectNode;
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON content in " + path, e);
        }
    }

    private static ColumnType inferColumnType(JsonNode node) {
        if (node == null || node.isNull()) {
            return ColumnType.VARCHAR;
        }
        if (node.isBoolean()) {
            return ColumnType.BOOLEAN;
        }
        if (node.isIntegralNumber() && node.canConvertToLong()) {
            return ColumnType.BIGINT;
        }
        if (node.isNumber()) {
            return ColumnType.DOUBLE;
        }
        if (node.isContainerNode()) {
            return ColumnType.VARCHAR;
        }
        return ColumnType.VARCHAR;
    }

    public record ColumnsMetadata(List<String> names, List<ColumnType> types) {
        public ColumnsMetadata {
            names = List.copyOf(requireNonNull(names, "names is null"));
            types = List.copyOf(requireNonNull(types, "types is null"));
            if (names.size() != types.size()) {
                throw new IllegalArgumentException("Column names and types must have the same size");
            }
        }
    }

    public record ColumnDefinition(String name, ColumnType type) {
        public ColumnDefinition {
            name = requireNonNull(name, "name is null");
            type = requireNonNull(type, "type is null");
        }
    }

    public enum ColumnType {
        BOOLEAN(BooleanType.BOOLEAN) {
            @Override
            void write(io.trino.spi.block.BlockBuilder blockBuilder, JsonNode node) {
                if (!node.isBoolean()) {
                    blockBuilder.appendNull();
                    return;
                }
                ((BooleanType) type).writeBoolean(blockBuilder, node.booleanValue());
            }
        },
        BIGINT(BigintType.BIGINT) {
            @Override
            void write(io.trino.spi.block.BlockBuilder blockBuilder, JsonNode node) {
                if (!(node.isIntegralNumber() || node.canConvertToLong())) {
                    blockBuilder.appendNull();
                    return;
                }
                ((BigintType) type).writeLong(blockBuilder, node.longValue());
            }
        },
        DOUBLE(DoubleType.DOUBLE) {
            @Override
            void write(io.trino.spi.block.BlockBuilder blockBuilder, JsonNode node) {
                if (!node.isNumber()) {
                    blockBuilder.appendNull();
                    return;
                }
                ((DoubleType) type).writeDouble(blockBuilder, node.doubleValue());
            }
        },
        VARCHAR(VarcharType.createUnboundedVarcharType()) {
            @Override
            void write(io.trino.spi.block.BlockBuilder blockBuilder, JsonNode node) {
                String text = node.isTextual() ? node.textValue() : node.toString();
                ((VarcharType) type).writeSlice(blockBuilder, io.airlift.slice.Slices.utf8Slice(text));
            }
        };

        protected final Type type;

        ColumnType(Type type) {
            this.type = type;
        }

        Type trinoType() {
            return type;
        }

        abstract void write(io.trino.spi.block.BlockBuilder blockBuilder, JsonNode node);

        static ColumnType fromSpecification(String value) {
            String normalized = value.trim().toUpperCase(Locale.ROOT);
            return switch (normalized) {
                case "BOOLEAN", "BOOL" -> BOOLEAN;
                case "BIGINT", "LONG", "INT64" -> BIGINT;
                case "DOUBLE", "FLOAT", "DECIMAL" -> DOUBLE;
                case "VARCHAR", "STRING", "JSON", "TEXT" -> VARCHAR;
                default -> throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported additional column type: " + value);
            };
        }
    }
}
