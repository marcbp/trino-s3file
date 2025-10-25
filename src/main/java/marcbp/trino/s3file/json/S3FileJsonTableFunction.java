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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3ObjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that streams newline-delimited JSON objects from S3-compatible storage.
 */
public final class S3FileJsonTableFunction extends AbstractConnectorTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileJsonTableFunction.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PATH_ARGUMENT = "PATH";
    private static final int DEFAULT_BATCH_SIZE = 512;
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ObjectService s3ObjectService;

    public S3FileJsonTableFunction(S3ObjectService s3ObjectService) {
        super(
                "json",
                "load",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name(PATH_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ObjectService = requireNonNull(s3ObjectService, "s3ObjectService is null");
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

        List<String> columnNames;
        try (BufferedReader reader = s3ObjectService.openReader(s3Path)) {
            columnNames = inferColumnNames(reader, s3Path);
        }
        catch (IOException e) {
            LOG.error("Failed to analyze json.load for {}", s3Path, e);
            throw new UncheckedIOException("Failed to inspect JSON data", e);
        }

        LOG.info("Detected {} JSON field(s) for path {}: {}", columnNames.size(), s3Path, columnNames);
        List<Type> columnTypes = columnNames.stream()
                .map(name -> (Type) VarcharType.createUnboundedVarcharType())
                .toList();
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        long fileSize = s3ObjectService.getObjectSize(s3Path);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, columnNames, null, fileSize, DEFAULT_SPLIT_SIZE_BYTES))
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

    public TableFunctionSplitProcessor createSplitProcessor(Handle handle, ConnectorSplit split) {
        if (!(split instanceof Split jsonSplit)) {
            throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
        }
        return new SplitProcessor(new Processor(s3ObjectService, handle, jsonSplit));
    }

    private static List<String> inferColumnNames(BufferedReader reader, String path) throws IOException {
        Set<String> orderedNames = new LinkedHashSet<>();
        String line;
        while ((line = reader.readLine()) != null) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }
            ObjectNode objectNode = parseObject(line, path);
            objectNode.fieldNames().forEachRemaining(orderedNames::add);
            if (!orderedNames.isEmpty()) {
                break;
            }
        }
        if (orderedNames.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "No JSON object found in " + path);
        }
        return List.copyOf(orderedNames);
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
        private final Integer batchSize;
        private final long fileSize;
        private final int splitSizeBytes;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes) {
            this.s3Path = requireNonNull(s3Path, "s3Path is null");
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.batchSize = batchSize;
            this.fileSize = fileSize;
            this.splitSizeBytes = splitSizeBytes;
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

        public List<Type> resolveColumnTypes() {
            List<Type> types = new ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                types.add(VarcharType.createUnboundedVarcharType());
            }
            return List.copyOf(types);
        }

        public int batchSizeOrDefault() {
            return batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
        }
    }

    private final class ProcessorProvider implements TableFunctionProcessorProvider {
        @Override
        public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle) {
            if (!(handle instanceof Handle jsonHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            LOG.info("Creating JSON data processor for path {}", jsonHandle.getS3Path());
            return new Processor(s3ObjectService, jsonHandle, null);
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split) {
            if (!(handle instanceof Handle jsonHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            if (!(split instanceof Split jsonSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return createSplitProcessor(jsonHandle, jsonSplit);
        }
    }

    private static final class Processor implements TableFunctionDataProcessor {
        private final S3ObjectService s3ObjectService;
        private final Handle handle;
        private final Split split;
        private final List<Type> columnTypes;
        private final List<String> columnNames;
        private final Charset charset = StandardCharsets.UTF_8;
        private final byte[] lineBreakBytes = "\n".getBytes(StandardCharsets.UTF_8);
        private final long primaryLength;
        private BufferedReader reader;
        private boolean finished;
        private boolean skipFirstLine;
        private long bytesWithinPrimary;

        private Processor(S3ObjectService s3ObjectService, Handle handle, Split split) {
            this.s3ObjectService = requireNonNull(s3ObjectService, "s3ObjectService is null");
            this.handle = requireNonNull(handle, "handle is null");
            this.split = split != null ? split : Split.forWholeFile(handle.getFileSize());
            this.columnTypes = handle.resolveColumnTypes();
            this.columnNames = handle.getColumns();
            this.primaryLength = this.split.getPrimaryLength();
            this.skipFirstLine = this.split.getStartOffset() > 0;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (finished) {
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                if (primaryLength == 0) {
                    finished = true;
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
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error("Error while reading JSON content for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to read JSON content", e);
            }
            catch (RuntimeException e) {
                LOG.error("Unexpected runtime error for path {}", handle.getS3Path(), e);
                throw e;
            }
        }

        private void ensureReader() {
            if (reader != null || finished) {
                return;
            }
            if (primaryLength == 0) {
                finished = true;
                return;
            }
            LOG.info("Opening JSON stream for path {} (split {}-{})", handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive());
            reader = s3ObjectService.openReader(handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive());
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
                String text = valueNode.isTextual() ? valueNode.asText() : valueNode.toString();
                ((VarcharType) columnTypes.get(columnIndex)).writeSlice(blockBuilder, Slices.utf8Slice(text));
            }
            pageBuilder.declarePosition();
        }

        private void completeProcessing() {
            closeReader();
            finished = true;
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
