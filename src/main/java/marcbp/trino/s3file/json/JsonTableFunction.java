package marcbp.trino.s3file.json;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AbstractDelimitedFilePageSource;
import marcbp.trino.s3file.file.BaseTextFileHandle;
import marcbp.trino.s3file.file.ByteDelimitedRecordReader;
import marcbp.trino.s3file.file.FileSelection;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.PageSettings;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnType;
import marcbp.trino.s3file.json.JsonFormatSupport.ColumnsMetadata;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3ClientConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.fileArguments;
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
    private final S3ClientBuilder s3ClientBuilder;
    private final int defaultSplitSizeBytes;
    private final PageSettings pageSettings;
    private final Logger logger = Logger.get(JsonTableFunction.class);

    public JsonTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, S3ClientConfig.DEFAULT_SPLIT_SIZE_BYTES, PageSettings.DEFAULT);
    }

    public JsonTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes) {
        this(s3ClientBuilder, defaultSplitSizeBytes, PageSettings.DEFAULT);
    }

    public JsonTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes, PageSettings pageSettings) {
        super(
                "json",
                "load",
                fileArguments(
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
        this.pageSettings = requireNonNull(pageSettings, "pageSettings is null");
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

        List<String> columnNames;
        List<ColumnType> detectedTypes;
        FileSelection selection;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session)) {
            selection = FileSelection.resolve(s3, s3Path, arguments);
            ColumnsMetadata columnsMetadata = selection.infer(s3, charset, (reader, object) ->
                    JsonFormatSupport.inferColumns(reader, object.path(), schemaSampleRows));
            columnNames = new ArrayList<>(columnsMetadata.names());
            detectedTypes = new ArrayList<>(columnsMetadata.types());
            JsonFormatSupport.mergeAdditionalColumns(columnNames, detectedTypes, JsonFormatSupport.parseAdditionalColumns(arguments));
        }
        catch (IOException e) {
            logger.error(e, "Failed to analyze json for %s", s3Path);
            throw new UncheckedIOException("Failed to inspect JSON data", e);
        }

        logger.info("Detected %s JSON field(s) for path %s after sampling %s row(s): %s",
                columnNames.size(), s3Path, schemaSampleRows, JsonFormatSupport.describeColumns(columnNames, detectedTypes));

        Handle handle = new Handle(
                selection,
                new ScanSettings(splitSizeBytes, pageSettings, charset.name()),
                new JsonSchema(columnNames, detectedTypes));
        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor.descriptor(handle.columnNames(), handle.resolveColumnTypes()))
                .handle(handle)
                .build();
    }

    private static int resolveSchemaSampleRows(Map<String, Argument> arguments) {
        ScalarArgument sampleRowsArgument = (ScalarArgument) arguments.get(SCHEMA_SAMPLE_ROWS_ARGUMENT);
        if (sampleRowsArgument == null) {
            return (int) DEFAULT_SCHEMA_SAMPLE_ROWS;
        }

        Object rawValue = sampleRowsArgument.getValue();
        if (!(rawValue instanceof Number sampleRowsValue)) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS must be an integer");
        }

        long sampleRows = sampleRowsValue.longValue();
        if (sampleRows <= 0) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS must be a positive integer");
        }
        if (sampleRows > Integer.MAX_VALUE) {
            throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "SCHEMA_SAMPLE_ROWS is too large");
        }
        return (int) sampleRows;
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, FileSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, split, columns);
    }

    public record JsonSchema(
            @JsonProperty("columns") List<String> columns,
            @JsonProperty("columnTypes") List<ColumnType> columnTypes) {
        @JsonCreator
        public JsonSchema {
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
            columnTypes = List.copyOf(requireNonNull(columnTypes, "columnTypes is null"));
        }
    }

    public static final class Handle extends BaseTextFileHandle {
        private final JsonSchema schema;

        public Handle(
                FileSelection selection,
                ScanSettings scan,
                JsonSchema schema) {
            super(selection, scan);
            this.schema = requireNonNull(schema, "schema is null");
        }

        @JsonCreator
        public Handle(
                @JsonProperty("pathColumn") String pathColumn,
                @JsonProperty("scan") ScanSettings scan,
                @JsonProperty("schema") JsonSchema schema) {
            super(pathColumn, scan);
            this.schema = requireNonNull(schema, "schema is null");
        }

        @Override
        public String format() {
            return "json";
        }

        @JsonProperty("schema")
        public JsonSchema schema() {
            return schema;
        }

        @Override
        public List<String> dataColumnNames() {
            return schema.columns();
        }

        @Override
        public List<Type> dataColumnTypes() {
            List<Type> types = new ArrayList<>(schema.columnTypes().size());
            for (ColumnType columnType : schema.columnTypes()) {
                types.add(columnType.trinoType());
            }
            return List.copyOf(types);
        }
    }

    private static final class PageSource extends AbstractDelimitedFilePageSource<Handle, Object[]> {
        private final List<String> columnNames;
        private final List<ColumnType> columnKinds;
        private final List<ColumnType> projectedColumnKinds;
        private final Map<String, Integer> columnIndexes;

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns, "\n", true);
            this.columnNames = handle.schema().columns();
            this.columnKinds = handle.schema().columnTypes();
            this.projectedColumnKinds = this.projectedColumns.stream()
                    .map(column -> columnKinds.get(column.getOrdinalPosition()))
                    .toList();
            this.columnIndexes = buildColumnIndexes();
        }

        private Map<String, Integer> buildColumnIndexes() {
            Map<String, Integer> fields = new HashMap<>();
            for (int i = 0; i < columnNames.size(); i++) {
                fields.put(columnNames.get(i), i);
            }
            return Map.copyOf(fields);
        }

        @Override
        protected RecordReadResult<Object[]> readNextRecord() throws IOException {
            Optional<ByteDelimitedRecordReader.Record> record = readLine();
            if (record.isEmpty()) {
                return RecordReadResult.finished();
            }
            ByteDelimitedRecordReader.Record lineRecord = record.orElseThrow();
            long lineBytes = lineRecord.bytesConsumed();
            if (lineRecord.isBlank(charset)) {
                return RecordReadResult.skip(lineBytes);
            }
            Object[] values = JsonFormatSupport.parseRecord(
                    lineRecord.valueBytes(),
                    charset,
                    split.object().path(),
                    columnIndexes,
                    columnKinds);
            return RecordReadResult.produce(values, lineBytes);
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object[] values) {
            for (int outputIndex = 0; outputIndex < projectedColumns.size(); outputIndex++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputIndex);
                projectedColumnKinds.get(outputIndex).writeValue(blockBuilder, values[projectedColumns.get(outputIndex).getOrdinalPosition()]);
            }
            pageBuilder.declarePosition();
        }
    }
}
