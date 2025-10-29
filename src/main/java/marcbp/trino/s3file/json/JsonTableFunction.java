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
import io.trino.spi.connector.ConnectorSplit;
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
import marcbp.trino.s3file.util.S3ClientBuilder;
import marcbp.trino.s3file.util.CharsetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(JsonTableFunction.class);
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
            LOG.error("Failed to analyze json.load for {}", s3Path, e);
            throw new UncheckedIOException("Failed to inspect JSON data", e);
        }

        LOG.info("Detected {} JSON field(s) for path {}: {}", columnNames.size(), s3Path, describeColumns(columnNames, detectedTypes));
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

    public List<ConnectorSplit> createSplits(Handle handle) {
        if (handle.getFileSize() == 0) {
            return List.of(Split.forWholeFile(0));
        }
        List<ConnectorSplit> splits = new ArrayList<>();
        long offset = 0;
        int index = 0;
        while (offset < handle.getFileSize()) {
            long primaryEnd = Math.min(handle.getFileSize(), offset + handle.getSplitSizeBytes());
            long rangeEnd = primaryEnd;
            boolean last = primaryEnd >= handle.getFileSize();
            if (!last) {
                rangeEnd = Math.min(handle.getFileSize(), primaryEnd + LOOKAHEAD_BYTES);
                last = rangeEnd >= handle.getFileSize();
            }
            splits.add(new Split(
                    "split-" + index,
                    offset,
                    primaryEnd,
                    last ? handle.getFileSize() : rangeEnd,
                    offset == 0,
                    last));
            offset = primaryEnd;
            index++;
        }
        return splits;
    }

    public TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, ConnectorSplit split) {
        if (!(split instanceof Split jsonSplit)) {
            throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
        }
        return new SplitProcessor(new Processor(session, s3ClientBuilder, handle, jsonSplit));
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

    public static final class Handle implements ConnectorTableFunctionHandle {
        private final String s3Path;
        private final List<String> columns;
        private final List<ColumnType> columnTypes;
        private final Integer batchSize;
        private final long fileSize;
        private final int splitSizeBytes;
        private final String charsetName;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("columnTypes") List<ColumnType> columnTypes,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName) {
            this.s3Path = requireNonNull(s3Path, "s3Path is null");
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
            this.batchSize = batchSize;
            this.fileSize = fileSize;
            this.splitSizeBytes = splitSizeBytes;
            this.charsetName = (charsetName == null || charsetName.isBlank()) ? StandardCharsets.UTF_8.name() : charsetName;
            if (this.columns.size() != this.columnTypes.size()) {
                throw new IllegalArgumentException("columns and columnTypes must have the same size");
            }
        }

        @JsonProperty
        public String getS3Path() {
            return s3Path;
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

        @JsonProperty
        public long getFileSize() {
            return fileSize;
        }

        @JsonProperty
        public int getSplitSizeBytes() {
            return splitSizeBytes;
        }

        @JsonProperty("charset")
        public String getCharsetName() {
            return charsetName;
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

        public Charset charset() {
            return CharsetUtils.resolve(charsetName);
        }
    }

    private final class ProcessorProvider implements TableFunctionProcessorProvider {
        @Override
        public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle) {
            if (!(handle instanceof Handle jsonHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            LOG.info("Creating JSON data processor for path {}", jsonHandle.getS3Path());
            return new Processor(session, s3ClientBuilder, jsonHandle, null);
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split) {
            if (!(handle instanceof Handle jsonHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            if (!(split instanceof Split jsonSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return createSplitProcessor(session, jsonHandle, jsonSplit);
        }
    }

    private static final class Processor implements TableFunctionDataProcessor {
        private final ConnectorSession session;
        private final S3ClientBuilder.SessionClient sessionClient;
        private final Handle handle;
        private final Split split;
        private final List<Type> columnTypes;
        private final List<String> columnNames;
        private final List<ColumnType> columnKinds;
        private final Charset charset;
        private final byte[] lineBreakBytes;
        private final long primaryLength;
        private BufferedReader reader;
        private boolean finished;
        private boolean skipFirstLine;
        private long bytesWithinPrimary;
        private boolean sessionClosed;

        private Processor(ConnectorSession session, S3ClientBuilder s3ClientBuilder, Handle handle, Split split) {
            this.session = requireNonNull(session, "session is null");
            this.sessionClient = s3ClientBuilder.forSession(session);
            this.handle = requireNonNull(handle, "handle is null");
            this.split = split != null ? split : Split.forWholeFile(handle.getFileSize());
            this.columnTypes = handle.resolveColumnTypes();
            this.columnNames = handle.getColumns();
            this.columnKinds = handle.getColumnTypes();
            this.primaryLength = this.split.getPrimaryLength();
            this.skipFirstLine = this.split.getStartOffset() > 0;
            this.charset = handle.charset();
            this.lineBreakBytes = "\n".getBytes(this.charset);
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (finished) {
                    closeSession();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                if (primaryLength == 0) {
                    finished = true;
                    closeSession();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                ensureReader();
                if (finished) {
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                PageBuilder pageBuilder = new PageBuilder(handle.batchSizeOrDefault(), columnTypes);
                while (!pageBuilder.isFull()) {
                    LineRead record = nextRecord();
                    if (record == null) {
                        completeProcessing();
                        break;
                    }
                    if (record.value().isEmpty()) {
                        continue;
                    }
                    ObjectNode objectNode = parseObject(record.value(), handle.getS3Path());
                    appendRow(pageBuilder, objectNode);
                    if (record.finishesSplit()) {
                        completeProcessing();
                        break;
                    }
                }

                if (pageBuilder.isEmpty()) {
                    finished = true;
                    closeSession();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error("Error while reading JSON content for path {}", handle.getS3Path(), e);
                closeSession();
                throw new UncheckedIOException("Failed to read JSON content", e);
            }
            catch (RuntimeException e) {
                LOG.error("Unexpected runtime error for path {}", handle.getS3Path(), e);
                closeSession();
                throw e;
            }
        }

        private void ensureReader() {
            if (reader != null || finished) {
                return;
            }
            if (primaryLength == 0) {
                finished = true;
                closeSession();
                return;
            }
            LOG.info("Opening JSON stream for path {} (split {}-{})", handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive());
            reader = sessionClient.openReader(handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive(), charset);
        }

        private LineRead nextRecord() throws IOException {
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }
                long lineBytes = calculateLineBytes(line);
                if (skipFirstLine) {
                    skipFirstLine = false;
                    bytesWithinPrimary += lineBytes;
                    continue;
                }
                boolean finishesSplit = false;
                if (!split.isLast() && bytesWithinPrimary + lineBytes > primaryLength) {
                    finishesSplit = true;
                }
                bytesWithinPrimary += lineBytes;
                return new LineRead(line, finishesSplit);
            }
        }

        private void appendRow(PageBuilder pageBuilder, ObjectNode objectNode) {
            for (int columnIndex = 0; columnIndex < columnTypes.size(); columnIndex++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnIndex);
                String column = columnNames.get(columnIndex);
                JsonNode valueNode = objectNode.get(column);
                if (valueNode == null || valueNode instanceof NullNode || valueNode.isNull()) {
                    blockBuilder.appendNull();
                    continue;
                }
                columnKinds.get(columnIndex).write(blockBuilder, valueNode);
            }
            pageBuilder.declarePosition();
        }

        private void completeProcessing() {
            closeReader();
            finished = true;
            closeSession();
        }

        private void closeReader() {
            if (reader == null) {
                return;
            }
            try {
                reader.close();
            }
            catch (IOException e) {
                LOG.error("Error closing JSON stream for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to close JSON stream", e);
            }
            finally {
                reader = null;
            }
        }

        private void closeSession() {
            if (sessionClosed) {
                return;
            }
            sessionClosed = true;
            sessionClient.close();
        }

        private long calculateLineBytes(String value) {
            return value.getBytes(charset).length + lineBreakBytes.length;
        }
    }

    private record LineRead(String value, boolean finishesSplit) {}

    public static final class Split implements ConnectorSplit {
        private final String id;
        private final long startOffset;
        private final long primaryEndOffset;
        private final long rangeEndExclusive;
        private final boolean first;
        private final boolean last;

        @JsonCreator
        public Split(@JsonProperty("id") String id,
                     @JsonProperty("startOffset") long startOffset,
                     @JsonProperty("primaryEndOffset") long primaryEndOffset,
                     @JsonProperty("rangeEndExclusive") long rangeEndExclusive,
                     @JsonProperty("first") boolean first,
                     @JsonProperty("last") boolean last) {
            this.id = requireNonNull(id, "id is null");
            this.startOffset = startOffset;
            this.primaryEndOffset = primaryEndOffset;
            this.rangeEndExclusive = rangeEndExclusive;
            this.first = first;
            this.last = last;
        }

        public static Split forWholeFile(long size) {
            return new Split("split-0", 0, size, size, true, true);
        }

        @JsonProperty
        public String getId() {
            return id;
        }

        @JsonProperty("startOffset")
        public long getStartOffset() {
            return startOffset;
        }

        @JsonProperty("primaryEndOffset")
        public long getPrimaryEndOffset() {
            return primaryEndOffset;
        }

        @JsonProperty("rangeEndExclusive")
        public long getRangeEndExclusive() {
            return rangeEndExclusive;
        }

        @JsonProperty("first")
        public boolean isFirst() {
            return first;
        }

        @JsonProperty("last")
        public boolean isLast() {
            return last;
        }

        public long getPrimaryLength() {
            return Math.max(0, primaryEndOffset - startOffset);
        }
    }

    private static final class SplitProcessor implements TableFunctionSplitProcessor {
        private final Processor processor;

        private SplitProcessor(Processor processor) {
            this.processor = requireNonNull(processor, "processor is null");
        }

        @Override
        public TableFunctionProcessorState process() {
            return processor.process(null);
        }
    }
}
