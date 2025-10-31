package marcbp.trino.s3file.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.file.AbstractFileProcessor;
import marcbp.trino.s3file.file.BaseFileHandle;
import marcbp.trino.s3file.file.BaseFileProcessorProvider;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.FileSplitProcessor;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.util.S3ClientBuilder;
import marcbp.trino.s3file.util.CharsetUtils;
import io.airlift.log.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Locale;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that streams newline-delimited JSON objects from S3-compatible storage.
 */
public final class JsonTableFunction extends AbstractConnectorTableFunction {
    private static final Logger LOG = Logger.get(JsonTableFunction.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PATH_ARGUMENT = "PATH";
    private static final String ENCODING_ARGUMENT = "ENCODING";
    private static final String ADDITIONAL_COLUMNS_ARGUMENT = "ADDITIONAL_COLUMNS";
    private static final int DEFAULT_BATCH_SIZE = 512;
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;
    private static final VarcharType UNBOUNDED_VARCHAR = VarcharType.createUnboundedVarcharType();

    private final S3ClientBuilder s3ClientBuilder;

    public JsonTableFunction(S3ClientBuilder s3ClientBuilder) {
        super(
                "json",
                "load",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name(PATH_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(ENCODING_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(StandardCharsets.UTF_8.name()))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(ADDITIONAL_COLUMNS_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(""))
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle,
                                         Map<String, Argument> arguments,
                                         ConnectorAccessControl accessControl) {
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

        Charset charset = CharsetUtils.resolve(arguments, ENCODING_ARGUMENT);

        List<String> columnNames;
        List<ColumnType> detectedTypes;
        long fileSize;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session);
             BufferedReader reader = s3.openReader(s3Path, charset)) {
            ColumnsMetadata columnsMetadata = inferColumns(reader, s3Path);
            columnNames = new ArrayList<>(columnsMetadata.names());
            detectedTypes = new ArrayList<>(columnsMetadata.types());

            List<ColumnDefinition> additionalColumns = parseAdditionalColumns(arguments);
            mergeAdditionalColumns(columnNames, detectedTypes, additionalColumns);
            fileSize = s3.getObjectSize(s3Path);
        }
        catch (IOException e) {
            LOG.error(e, "Failed to analyze json.load for %s", s3Path);
            throw new UncheckedIOException("Failed to inspect JSON data", e);
        }

        LOG.info("Detected %s JSON field(s) for path %s: %s", columnNames.size(), s3Path, describeColumns(columnNames, detectedTypes));
        List<Type> columnTypes = new ArrayList<>(detectedTypes.size());
        for (ColumnType columnType : detectedTypes) {
            columnTypes.add(columnType.trinoType());
        }
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, columnNames, detectedTypes, null, fileSize, DEFAULT_SPLIT_SIZE_BYTES, charset.name()))
                .build();
    }

    public TableFunctionProcessorProvider createProcessorProvider() {
        return new ProcessorProvider();
    }

    public List<FileSplit> createSplits(Handle handle) {
        return SplitPlanner.planSplits(handle.getFileSize(), handle.getSplitSizeBytes(), LOOKAHEAD_BYTES);
    }

    public TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
        return new FileSplitProcessor(new Processor(session, s3ClientBuilder, handle, split));
    }

    private static ColumnsMetadata inferColumns(BufferedReader reader, String path) throws IOException {
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

    private static List<ColumnDefinition> parseAdditionalColumns(Map<String, Argument> arguments) {
        Argument argument = arguments.get(ADDITIONAL_COLUMNS_ARGUMENT);
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

    private static void mergeAdditionalColumns(List<String> names, List<ColumnType> types, List<ColumnDefinition> additionalColumns) {
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
            // Trino does not expose io.trino.spi.type.JsonType via the SPI,
            // so the connector cannot produce native JSON blocks. Fall back to text.
            return ColumnType.VARCHAR;
        }
        return ColumnType.VARCHAR;
    }

    private static List<String> describeColumns(List<String> names, List<ColumnType> types) {
        List<String> summary = new ArrayList<>(names.size());
        for (int i = 0; i < names.size(); i++) {
            summary.add(names.get(i) + ":" + types.get(i).name().toLowerCase(Locale.ROOT));
        }
        return summary;
    }

    private record ColumnsMetadata(List<String> names, List<ColumnType> types) {
        private ColumnsMetadata {
            names = List.copyOf(requireNonNull(names, "names is null"));
            types = List.copyOf(requireNonNull(types, "types is null"));
            if (names.size() != types.size()) {
                throw new IllegalArgumentException("Column names and types must have the same size");
            }
        }
    }

    private record ColumnDefinition(String name, ColumnType type) {
        private ColumnDefinition {
            name = requireNonNull(name, "name is null");
            type = requireNonNull(type, "type is null");
        }
    }

    private enum ColumnType {
        BOOLEAN(BooleanType.BOOLEAN) {
            @Override
            void write(BlockBuilder blockBuilder, JsonNode node) {
                if (!node.isBoolean()) {
                    blockBuilder.appendNull();
                    return;
                }
                ((BooleanType) type).writeBoolean(blockBuilder, node.booleanValue());
            }
        },
        BIGINT(BigintType.BIGINT) {
            @Override
            void write(BlockBuilder blockBuilder, JsonNode node) {
                if (!(node.isIntegralNumber() || node.canConvertToLong())) {
                    blockBuilder.appendNull();
                    return;
                }
                ((BigintType) type).writeLong(blockBuilder, node.longValue());
            }
        },
        DOUBLE(DoubleType.DOUBLE) {
            @Override
            void write(BlockBuilder blockBuilder, JsonNode node) {
                if (!node.isNumber()) {
                    blockBuilder.appendNull();
                    return;
                }
                ((DoubleType) type).writeDouble(blockBuilder, node.doubleValue());
            }
        },
        VARCHAR(UNBOUNDED_VARCHAR) {
            @Override
            void write(BlockBuilder blockBuilder, JsonNode node) {
                String text = node.isTextual() ? node.textValue() : node.toString();
                UNBOUNDED_VARCHAR.writeSlice(blockBuilder, Slices.utf8Slice(text));
            }
        };

        protected final Type type;

        ColumnType(Type type) {
            this.type = type;
        }

        Type trinoType() {
            return type;
        }

        abstract void write(BlockBuilder blockBuilder, JsonNode node);

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

    private static ObjectNode parseObject(String json, String path) {
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

    public static final class Handle extends BaseFileHandle {
        private final List<String> columns;
        private final List<ColumnType> columnTypes;
        private final Integer batchSize;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("columnTypes") List<ColumnType> columnTypes,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName) {
            super(s3Path, fileSize, splitSizeBytes, charsetName);
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.batchSize = batchSize;
        }

        @JsonProperty
        public List<String> getColumns() {
            return columns;
        }

        @JsonProperty
        public List<ColumnType> getColumnTypes() {
            return columnTypes;
        }

        @JsonProperty
        public Integer getBatchSize() {
            return batchSize;
        }

        public List<Type> resolveColumnTypes() {
            List<Type> types = new ArrayList<>(columnTypes.size());
            for (ColumnType columnType : columnTypes) {
                types.add(columnType.trinoType());
            }
            return List.copyOf(types);
        }

        public int batchSizeOrDefault() {
            return batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
        }
    }

    private final class ProcessorProvider extends BaseFileProcessorProvider<Handle> {
        private ProcessorProvider() {
            super(Handle.class);
        }

        @Override
        protected TableFunctionDataProcessor createDataProcessor(ConnectorSession session, Handle handle) {
            LOG.info("Creating JSON data processor for path %s", handle.getS3Path());
            return new Processor(session, s3ClientBuilder, handle, null);
        }

        @Override
        protected TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
            return JsonTableFunction.this.createSplitProcessor(session, handle, split);
        }
    }

    private static final class Processor extends AbstractFileProcessor<Handle> {
        private final List<Type> columnTypes;
        private final List<String> columnNames;
        private final List<ColumnType> columnKinds;
        private final byte[] lineBreakBytes;

        private Processor(ConnectorSession session, S3ClientBuilder s3ClientBuilder, Handle handle, FileSplit split) {
            super(session, s3ClientBuilder, handle, split);
            this.columnTypes = handle.resolveColumnTypes();
            this.columnNames = handle.getColumns();
            this.columnKinds = handle.getColumnTypes();
            this.lineBreakBytes = "\n".getBytes(charset());
        }

        @Override
        protected BufferedReader openReader() {
            if (split().isWholeFile()) {
                return sessionClient().openReader(handle().getS3Path(), charset());
            }
            return sessionClient().openReader(handle().getS3Path(), split().getStartOffset(), split().getRangeEndExclusive(), charset());
        }

        @Override
        protected void handleReaderCloseException(IOException e) {
            LOG.error(e, "Error closing JSON stream for path %s", handle().getS3Path());
            throw new UncheckedIOException("Failed to close JSON stream", e);
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (isFinished()) {
                    closeSession();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                if (primaryLength() == 0) {
                    markFinished();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                ensureReader();
                if (isFinished()) {
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                PageBuilder pageBuilder = new PageBuilder(handle().batchSizeOrDefault(), columnTypes);
                while (!pageBuilder.isFull()) {
                    LineRead record = nextRecord();
                    if (record == null) {
                        completeProcessing();
                        break;
                    }
                    if (record.value().isEmpty()) {
                        continue;
                    }
                    ObjectNode objectNode = parseObject(record.value(), handle().getS3Path());
                    appendRow(pageBuilder, objectNode);
                    if (record.finishesSplit()) {
                        completeProcessing();
                        break;
                    }
                }

                if (pageBuilder.isEmpty()) {
                    markFinished();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error(e, "Error while reading JSON content for path %s", handle().getS3Path());
                closeSession();
                throw new UncheckedIOException("Failed to read JSON content", e);
            }
            catch (RuntimeException e) {
                LOG.error(e, "Unexpected runtime error for path %s", handle().getS3Path());
                closeSession();
                throw e;
            }
        }

        private LineRead nextRecord() throws IOException {
            while (true) {
                String line = reader().readLine();
                if (line == null) {
                    return null;
                }
                long lineBytes = calculateLineBytes(line);
                boolean finishesSplit = false;
                if (!split().isLast() && bytesWithinPrimary() + lineBytes > primaryLength()) {
                    finishesSplit = true;
                }
                addBytesWithinPrimary(lineBytes);
                return new LineRead(line, finishesSplit);
            }
        }

        private void appendRow(PageBuilder pageBuilder, ObjectNode objectNode) {
            for (int i = 0; i < columnNames.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                ColumnType columnType = columnKinds.get(i);
                JsonNode value = objectNode.get(columnNames.get(i));
                columnType.write(blockBuilder, value == null ? NullNode.getInstance() : value);
            }
            pageBuilder.declarePosition();
        }

        private long calculateLineBytes(String value) {
            return value.getBytes(charset()).length + lineBreakBytes.length;
        }
    }

    private record LineRead(String value, boolean finishesSplit) {}
}
