package marcbp.trino.s3file.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.file.AbstractFileProcessor;
import marcbp.trino.s3file.file.BaseFileHandle;
import marcbp.trino.s3file.file.BaseFileProcessorProvider;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.FileSplitProcessor;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnDefinition;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnType;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnsMetadata;
import marcbp.trino.s3file.util.CharsetUtils;
import marcbp.trino.s3file.util.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that streams newline-delimited JSON objects from S3-compatible storage.
 */
public final class JsonTableFunction extends AbstractConnectorTableFunction {
    private static final String PATH_ARGUMENT = "PATH";
    private static final String ENCODING_ARGUMENT = "ENCODING";
    static final String ADDITIONAL_COLUMNS_ARGUMENT = "ADDITIONAL_COLUMNS";

    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;
    private final Logger logger = Logger.get(JsonTableFunction.class);

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
            ColumnsMetadata columnsMetadata = JsonFormatSupport.inferColumns(reader, s3Path);
            columnNames = new ArrayList<>(columnsMetadata.names());
            detectedTypes = new ArrayList<>(columnsMetadata.types());

            List<ColumnDefinition> additionalColumns = JsonFormatSupport.parseAdditionalColumns(arguments);
            JsonFormatSupport.mergeAdditionalColumns(columnNames, detectedTypes, additionalColumns);
            fileSize = s3.getObjectSize(s3Path);
        }
        catch (IOException e) {
            logger.error(e, "Failed to analyze json for %s", s3Path);
            throw new UncheckedIOException("Failed to inspect JSON data", e);
        }

        logger.info("Detected %s JSON field(s) for path %s: %s", columnNames.size(), s3Path, JsonFormatSupport.describeColumns(columnNames, detectedTypes));
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
            logger.info("Creating JSON data processor for path %s", handle.getS3Path());
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
            this.lineBreakBytes = "\n".getBytes(charset);
        }

        @Override
        protected RecordReadResult<?> readNextRecord() throws IOException {
            String line = reader().readLine();
            if (line == null) {
                return RecordReadResult.finished();
            }
            long lineBytes = calculateLineBytes(line);
            if (line.isEmpty()) {
                return RecordReadResult.skip(lineBytes);
            }
            boolean finishesSplit = !split.isLast() && bytesWithinPrimary + lineBytes > primaryLength;
            ObjectNode objectNode = JsonFormatSupport.parseObject(line, handle.getS3Path());
            return RecordReadResult.produce(objectNode, lineBytes, finishesSplit);
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object payload) {
            ObjectNode objectNode = (ObjectNode) payload;
            for (int i = 0; i < columnNames.size(); i++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                ColumnType columnType = columnKinds.get(i);
                JsonNode value = objectNode.get(columnNames.get(i));
                columnType.write(blockBuilder, value == null ? NullNode.getInstance() : value);
            }
            pageBuilder.declarePosition();
        }

        private long calculateLineBytes(String value) {
            return value.getBytes(charset).length + lineBreakBytes.length;
        }

        @Override
        protected List<Type> columnTypes() {
            return columnTypes;
        }

        @Override
        protected int batchSize() {
            return handle.batchSizeOrDefault();
        }

    }
}
