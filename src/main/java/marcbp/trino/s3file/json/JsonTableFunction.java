package marcbp.trino.s3file.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AbstractTextFilePageSource;
import marcbp.trino.s3file.file.BaseTextFileHandle;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.TextSplitBoundarySupport;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnDefinition;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnType;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnsMetadata;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.pathArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveSplitSizeBytes;
import static marcbp.trino.s3file.util.TableFunctionArguments.splitSizeMbArgumentSpecification;

/**
 * Table function that streams newline-delimited JSON objects from S3-compatible storage.
 */
public final class JsonTableFunction extends AbstractConnectorTableFunction {
    static final String ADDITIONAL_COLUMNS_ARGUMENT = "ADDITIONAL_COLUMNS";
    static final String SCHEMA_SAMPLE_ROWS_ARGUMENT = "SCHEMA_SAMPLE_ROWS";
    private static final long DEFAULT_SCHEMA_SAMPLE_ROWS = 100L;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;
    private final int defaultSplitSizeBytes;
    private final Logger logger = Logger.get(JsonTableFunction.class);

    public JsonTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, marcbp.trino.s3file.s3.S3ClientConfig.DEFAULT_SPLIT_SIZE_BYTES);
    }

    public JsonTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes) {
        super(
                "json",
                "load",
                List.of(
                        pathArgumentSpecification(),
                        encodingArgumentSpecification(),
                        splitSizeMbArgumentSpecification(),
                        ScalarArgumentSpecification.builder()
                                .name(SCHEMA_SAMPLE_ROWS_ARGUMENT)
                                .type(BigintType.BIGINT)
                                .defaultValue(DEFAULT_SCHEMA_SAMPLE_ROWS)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(ADDITIONAL_COLUMNS_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(""))
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
        this.defaultSplitSizeBytes = defaultSplitSizeBytes;
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle,
                                         Map<String, Argument> arguments,
                                         ConnectorAccessControl accessControl) {
        String s3Path = requirePath(arguments);
        Charset charset = resolveEncoding(arguments);
        int splitSizeBytes = resolveSplitSizeBytes(arguments, defaultSplitSizeBytes);
        int schemaSampleRows = resolveSchemaSampleRows(arguments);
        long analyzeStartedAt = System.nanoTime();

        List<String> columnNames;
        List<ColumnType> detectedTypes;
        int sampledRows;
        S3ClientBuilder.ObjectMetadata metadata;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session);
             BufferedReader reader = s3.openReader(s3Path, charset)) {
            metadata = s3.getObjectMetadata(s3Path);
            ColumnsMetadata columnsMetadata = JsonFormatSupport.inferColumns(reader, s3Path, schemaSampleRows);
            columnNames = new ArrayList<>(columnsMetadata.names());
            detectedTypes = new ArrayList<>(columnsMetadata.types());
            sampledRows = columnsMetadata.sampledRows();

            List<ColumnDefinition> additionalColumns = JsonFormatSupport.parseAdditionalColumns(arguments);
            JsonFormatSupport.mergeAdditionalColumns(columnNames, detectedTypes, additionalColumns);
        }
        catch (IOException e) {
            logger.error(e, "Failed to analyze json for %s", s3Path);
            throw new UncheckedIOException("Failed to inspect JSON data", e);
        }

        logger.info("Detected %s JSON field(s) for path %s after sampling %s row(s): %s",
                columnNames.size(), s3Path, schemaSampleRows, JsonFormatSupport.describeColumns(columnNames, detectedTypes));
        List<Type> columnTypes = new ArrayList<>(detectedTypes.size());
        for (ColumnType columnType : detectedTypes) {
            columnTypes.add(columnType.trinoType());
        }
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(
                        s3Path,
                        columnNames,
                        detectedTypes,
                        null,
                        metadata.size(),
                        splitSizeBytes,
                        charset.name(),
                        metadata.eTag().orElse(null),
                        metadata.versionId().orElse(null),
                        (long) sampledRows,
                        columnNames.size(),
                        System.nanoTime() - analyzeStartedAt))
                .build();
    }

    private static int resolveSchemaSampleRows(Map<String, Argument> arguments) {
        ScalarArgument sampleRowsArgument = (ScalarArgument) arguments.get(SCHEMA_SAMPLE_ROWS_ARGUMENT);
        if (sampleRowsArgument == null) {
            return (int) DEFAULT_SCHEMA_SAMPLE_ROWS;
        }

        Object rawValue = sampleRowsArgument.getValue();
        if (!(rawValue instanceof Number sampleRowsValue)) {
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS must be an integer");
        }

        long sampleRows = sampleRowsValue.longValue();
        if (sampleRows <= 0) {
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS must be a positive integer");
        }
        if (sampleRows > Integer.MAX_VALUE) {
            throw new io.trino.spi.TrinoException(io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS is too large");
        }
        return (int) sampleRows;
    }

    public List<FileSplit> createSplits(Handle handle) {
        return SplitPlanner.planSplits(handle.getFileSize(), handle.getSplitSizeBytes(), LOOKAHEAD_BYTES);
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, FileSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, split, columns);
    }

    public static final class Handle extends BaseTextFileHandle {
        private final List<String> columns;
        private final List<ColumnType> columnTypes;

        public Handle(
                String s3Path,
                List<String> columns,
                List<ColumnType> columnTypes,
                Integer batchSize,
                long fileSize,
                int splitSizeBytes,
                String charsetName,
                String eTag,
                String versionId) {
            this(s3Path, columns, columnTypes, batchSize, fileSize, splitSizeBytes, charsetName, eTag, versionId, 0L, 0, 0L);
        }

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("columnTypes") List<ColumnType> columnTypes,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName,
                      @JsonProperty("etag") String eTag,
                      @JsonProperty("versionId") String versionId,
                      @JsonProperty("analysisRowsSampled") Long analysisRowsSampled,
                      @JsonProperty("analysisColumnsDetected") Integer analysisColumnsDetected,
                      @JsonProperty("analysisTimeNanos") Long analysisTimeNanos) {
            super(
                    s3Path,
                    fileSize,
                    splitSizeBytes,
                    charsetName,
                    batchSize == null ? BaseTextFileHandle.DEFAULT_BATCH_SIZE : batchSize,
                    Optional.ofNullable(eTag),
                    Optional.ofNullable(versionId),
                    analysisRowsSampled == null ? 0 : analysisRowsSampled,
                    analysisColumnsDetected == null ? 0 : analysisColumnsDetected,
                    analysisTimeNanos == null ? 0 : analysisTimeNanos);
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        }

        @Override
        public String format() {
            return "json";
        }

        @JsonProperty
        public List<String> getColumns() {
            return columns;
        }

        @JsonProperty
        public List<ColumnType> getColumnTypes() {
            return columnTypes;
        }

        @Override
        public List<String> columnNames() {
            return columns;
        }

        @Override
        public List<Type> resolveColumnTypes() {
            List<Type> types = new ArrayList<>(columnTypes.size());
            for (ColumnType columnType : columnTypes) {
                types.add(columnType.trinoType());
            }
            return List.copyOf(types);
        }
    }

    private static final class PageSource extends AbstractTextFilePageSource<Handle> {
        private final List<String> columnNames;
        private final List<ColumnType> columnKinds;
        private final byte[] lineBreakBytes;
        private boolean skipFirstLine;

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns);
            this.columnNames = handle.getColumns();
            this.columnKinds = handle.getColumnTypes();
            this.lineBreakBytes = "\n".getBytes(charset);
            this.skipFirstLine = split.getStartOffset() > 0;
        }

        @Override
        protected boolean finishWhenEmptySplit() {
            return primaryLength == 0 && split.getStartOffset() > 0;
        }

        @Override
        protected void afterReaderOpened(BufferedReader reader) throws IOException {
            if (skipFirstLine) {
                recordS3Request();
                skipFirstLine = !TextSplitBoundarySupport.startsAtLineBoundary(sessionClient, handle, split);
            }
        }

        @Override
        protected RecordReadResult<?> readNextRecord() throws IOException {
            String line = reader().readLine();
            if (line == null) {
                return RecordReadResult.finished();
            }
            long lineBytes = line.getBytes(charset).length + lineBreakBytes.length;
            if (skipFirstLine) {
                skipFirstLine = false;
                return RecordReadResult.skip(lineBytes);
            }
            if (line.isBlank()) {
                return RecordReadResult.skip(lineBytes);
            }
            boolean finishesSplit = !split.isLast() && bytesWithinPrimary + lineBytes > primaryLength;
            ObjectNode objectNode = JsonFormatSupport.parseObject(line, handle.getS3Path());
            return RecordReadResult.produce(objectNode, lineBytes, finishesSplit);
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object payload) {
            ObjectNode objectNode = (ObjectNode) payload;
            for (int outputIndex = 0; outputIndex < projectedColumns.size(); outputIndex++) {
                S3FileColumnHandle columnHandle = projectedColumns.get(outputIndex);
                int sourceIndex = columnHandle.getOrdinalPosition();
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputIndex);
                ColumnType columnType = columnKinds.get(sourceIndex);
                JsonNode value = objectNode.get(columnNames.get(sourceIndex));
                columnType.write(blockBuilder, value == null ? NullNode.getInstance() : value);
            }
            pageBuilder.declarePosition();
        }
    }
}
