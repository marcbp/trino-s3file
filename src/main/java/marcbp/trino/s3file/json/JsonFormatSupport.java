package marcbp.trino.s3file.json;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
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
    private static final int DEFAULT_SCHEMA_SAMPLE_ROWS = 100;

    private JsonFormatSupport() {
    }

    public static ColumnsMetadata inferColumns(BufferedReader reader, String path) throws IOException {
        return inferColumns(reader, path, DEFAULT_SCHEMA_SAMPLE_ROWS);
    }

    public static ColumnsMetadata inferColumns(BufferedReader reader, String path, int sampleRows) throws IOException {
        if (sampleRows <= 0) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS must be a positive integer");
        }

        Map<String, ColumnType> columns = new LinkedHashMap<>();
        int sampledRows = 0;
        String line;
        while (sampledRows < sampleRows && (line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            ObjectNode objectNode = parseObject(line, path);
            objectNode.fieldNames().forEachRemaining(field -> {
                ColumnType observedType = inferColumnType(objectNode.get(field));
                columns.merge(field, observedType, JsonFormatSupport::mergeColumnTypes);
            });
            sampledRows++;
            if (!columns.isEmpty() && sampledRows >= sampleRows) {
                break;
            }
        }
        if (sampledRows == 0) {
            return null;
        }
        if (columns.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "No JSON object found in " + path);
        }

        return new ColumnsMetadata(new ArrayList<>(columns.keySet()), new ArrayList<>(columns.values()), sampledRows);
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

    public static Object[] parseRecord(
            byte[] jsonBytes,
            Charset charset,
            String path,
            Map<String, Integer> columnIndexes,
            List<ColumnType> columnTypes) {
        try (JsonParser parser = newParser(jsonBytes, charset)) {
            if (parser.nextToken() != JsonToken.START_OBJECT) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "JSON row must be an object in " + path);
            }

            Object[] values = new Object[columnTypes.size()];
            while (parser.nextToken() != JsonToken.END_OBJECT) {
                if (parser.currentToken() != JsonToken.FIELD_NAME) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON object in " + path);
                }
                String fieldName = parser.currentName();
                JsonToken valueToken = parser.nextToken();
                Integer index = columnIndexes.get(fieldName);
                if (index == null) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unexpected JSON field '" + fieldName + "' in " + path);
                }
                Object value = columnTypes.get(index).read(parser, valueToken);
                if (value == null && valueToken != JsonToken.VALUE_NULL) {
                    throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                            "Incompatible JSON type for field '" + fieldName + "' in " + path + ": expected " + columnTypes.get(index));
                }
                values[index] = value;
                if (valueToken != null && valueToken.isStructStart()) {
                    parser.skipChildren();
                }
            }
            if (parser.nextToken() != null) {
                throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON content in " + path);
            }
            return values;
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Invalid JSON content in " + path, e);
        }
        catch (IOException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Failed to read JSON content in " + path, e);
        }
    }

    private static JsonParser newParser(byte[] jsonBytes, Charset charset) throws IOException {
        if (StandardCharsets.UTF_8.equals(charset)) {
            return OBJECT_MAPPER.getFactory().createParser(jsonBytes);
        }
        return OBJECT_MAPPER.getFactory().createParser(new String(jsonBytes, charset));
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

    private static ColumnType mergeColumnTypes(ColumnType existing, ColumnType observed) {
        if (existing == observed) {
            return existing;
        }
        if (existing == ColumnType.VARCHAR || observed == ColumnType.VARCHAR) {
            return ColumnType.VARCHAR;
        }
        if ((existing == ColumnType.BIGINT && observed == ColumnType.DOUBLE) || (existing == ColumnType.DOUBLE && observed == ColumnType.BIGINT)) {
            return ColumnType.DOUBLE;
        }
        return ColumnType.VARCHAR;
    }

    public record ColumnsMetadata(List<String> names, List<ColumnType> types, int sampledRows) {
        public ColumnsMetadata {
            names = List.copyOf(requireNonNull(names, "names is null"));
            types = List.copyOf(requireNonNull(types, "types is null"));
            if (names.size() != types.size()) {
                throw new IllegalArgumentException("Column names and types must have the same size");
            }
            if (sampledRows < 0) {
                throw new IllegalArgumentException("sampledRows must be >= 0");
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
            Object read(JsonParser parser, JsonToken token) {
                if (token == JsonToken.VALUE_TRUE) {
                    return true;
                }
                if (token == JsonToken.VALUE_FALSE) {
                    return false;
                }
                return null;
            }

            @Override
            void writeValue(BlockBuilder blockBuilder, Object value) {
                if (!(value instanceof Boolean booleanValue)) {
                    blockBuilder.appendNull();
                    return;
                }
                ((BooleanType) type).writeBoolean(blockBuilder, booleanValue);
            }
        },
        BIGINT(BigintType.BIGINT) {
            @Override
            Object read(JsonParser parser, JsonToken token) throws IOException {
                if (token == null || !token.isNumeric()) {
                    return null;
                }
                return switch (parser.getNumberType()) {
                    case INT, LONG -> parser.getLongValue();
                    default -> null;
                };
            }

            @Override
            void writeValue(BlockBuilder blockBuilder, Object value) {
                if (!(value instanceof Long longValue)) {
                    blockBuilder.appendNull();
                    return;
                }
                ((BigintType) type).writeLong(blockBuilder, longValue);
            }
        },
        DOUBLE(DoubleType.DOUBLE) {
            @Override
            Object read(JsonParser parser, JsonToken token) throws IOException {
                if (token == null || !token.isNumeric()) {
                    return null;
                }
                return parser.getDoubleValue();
            }

            @Override
            void writeValue(BlockBuilder blockBuilder, Object value) {
                if (!(value instanceof Double doubleValue)) {
                    blockBuilder.appendNull();
                    return;
                }
                ((DoubleType) type).writeDouble(blockBuilder, doubleValue);
            }
        },
        VARCHAR(VarcharType.createUnboundedVarcharType()) {
            @Override
            Object read(JsonParser parser, JsonToken token) throws IOException {
                if (token == null || token == JsonToken.VALUE_NULL) {
                    return null;
                }
                if (token == JsonToken.VALUE_STRING) {
                    return parser.getText();
                }
                if (token.isStructStart()) {
                    return OBJECT_MAPPER.readTree(parser).toString();
                }
                return parser.getText();
            }

            @Override
            void writeValue(BlockBuilder blockBuilder, Object value) {
                if (!(value instanceof String text)) {
                    blockBuilder.appendNull();
                    return;
                }
                ((VarcharType) type).writeSlice(blockBuilder, Slices.utf8Slice(text));
            }
        };

        protected final Type type;

        ColumnType(Type type) {
            this.type = type;
        }

        Type trinoType() {
            return type;
        }

        abstract Object read(JsonParser parser, JsonToken token) throws IOException;

        abstract void writeValue(BlockBuilder blockBuilder, Object value);

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
