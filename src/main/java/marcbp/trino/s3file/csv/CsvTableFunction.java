package marcbp.trino.s3file.csv;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.opencsv.CSVParser;
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
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
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
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3ClientConfig;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.fileArguments;
import static marcbp.trino.s3file.util.TableFunctionArguments.optionalBoolean;
import static marcbp.trino.s3file.util.TableFunctionArguments.optionalString;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveSplitSizeBytes;
import static marcbp.trino.s3file.util.TableFunctionArguments.splitSizeMbArgumentSpecification;

/**
 * Table function that reads CSV data from S3-compatible storage, inferring columns on the fly.
 */
public final class CsvTableFunction extends AbstractConnectorTableFunction {
    private static final String DELIMITER_ARGUMENT = "DELIMITER";
    private static final String MULTILINE_ARGUMENT = "MULTILINE";

    private final S3ClientBuilder s3ClientBuilder;
    private final int defaultSplitSizeBytes;
    private final PageSettings pageSettings;
    private final Logger logger = Logger.get(CsvTableFunction.class);

    public CsvTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, S3ClientConfig.DEFAULT_SPLIT_SIZE_BYTES, PageSettings.DEFAULT);
    }

    public CsvTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes) {
        this(s3ClientBuilder, defaultSplitSizeBytes, PageSettings.DEFAULT);
    }

    public CsvTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes, PageSettings pageSettings) {
        super(
                "csv",
                "load",
                fileArguments(
                        ScalarArgumentSpecification.builder()
                                .name(DELIMITER_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(";"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("HEADER")
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("true"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(MULTILINE_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("false"))
                                .build(),
                        encodingArgumentSpecification(),
                        splitSizeMbArgumentSpecification()
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

        char delimiter = resolveDelimiter(arguments);

        logger.info("Analyzing load table function for path %s with delimiter %s", s3Path, delimiter);
        boolean headerPresent = optionalBoolean(arguments, "HEADER", true);
        boolean multiline = optionalBoolean(arguments, MULTILINE_ARGUMENT, false);
        logger.info("Header present: %s", headerPresent);

        List<String> columnNames;
        FileSelection selection;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session)) {
            selection = FileSelection.resolve(s3, s3Path, arguments);
            columnNames = selection.infer(s3, charset, (reader, object) ->
                    CsvFormatSupport.inferColumnNames(reader, object.path(), delimiter, headerPresent, multiline));
        }
        catch (IOException e) {
            logger.error(e, "Failed to infer column names for %s", s3Path);
            throw new UncheckedIOException("Failed to infer column names", e);
        }
        logger.info("Detected %s columns: %s", columnNames.size(), columnNames);

        Handle handle = new Handle(
                selection,
                new ScanSettings(splitSizeBytes, pageSettings, charset.name()),
                new CsvSchema(columnNames),
                new CsvOptions(delimiter, headerPresent, multiline));
        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor.descriptor(handle.columnNames(), handle.resolveColumnTypes()))
                .handle(handle)
                .build();
    }

    private static char resolveDelimiter(Map<String, Argument> arguments) {
        String delimiter = optionalString(arguments, DELIMITER_ARGUMENT, ";");
        if (delimiter.length() != 1) {
            throw new TrinoException(
                    StandardErrorCode.INVALID_FUNCTION_ARGUMENT,
                    "DELIMITER must be a single character");
        }
        return delimiter.charAt(0);
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, FileSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, split, columns);
    }

    public record CsvSchema(@JsonProperty("columns") List<String> columns) {
        @JsonCreator
        public CsvSchema {
            columns = List.copyOf(requireNonNull(columns, "columns is null"));
        }
    }

    public record CsvOptions(
            @JsonProperty("delimiter") char delimiter,
            @JsonProperty("header") boolean headerPresent,
            @JsonProperty("multiline") boolean multiline) {
        @JsonCreator
        public CsvOptions {}
    }

    public static final class Handle extends BaseTextFileHandle {
        private final CsvSchema schema;
        private final CsvOptions options;

        public Handle(
                FileSelection selection,
                ScanSettings scan,
                CsvSchema schema,
                CsvOptions options) {
            super(selection, scan);
            this.schema = requireNonNull(schema, "schema is null");
            this.options = requireNonNull(options, "options is null");
        }

        @JsonCreator
        public Handle(
                @JsonProperty("pathColumn") String pathColumn,
                @JsonProperty("scan") ScanSettings scan,
                @JsonProperty("schema") CsvSchema schema,
                @JsonProperty("options") CsvOptions options) {
            super(pathColumn, scan);
            this.schema = requireNonNull(schema, "schema is null");
            this.options = requireNonNull(options, "options is null");
        }

        @Override
        protected boolean splittable() {
            return !options.multiline();
        }

        @Override
        public String format() {
            return "csv";
        }

        @JsonProperty("schema")
        public CsvSchema schema() {
            return schema;
        }

        @JsonProperty("options")
        public CsvOptions options() {
            return options;
        }

        @Override
        public List<String> dataColumnNames() {
            return schema.columns();
        }

        @Override
        public List<Type> dataColumnTypes() {
            return Collections.nCopies(schema.columns().size(), VarcharType.VARCHAR);
        }
    }

    private static final class PageSource extends AbstractDelimitedFilePageSource<Handle, String[]> {
        private final CSVParser parser;

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns, "\n", true);
            this.parser = CsvFormatSupport.newParser(handle.options().delimiter());
        }

        @Override
        protected void afterSourceOpened() throws IOException {
            if (handle.options().headerPresent() && split.first()) {
                CsvRecord header = readCsvRecord();
                if (header != null) {
                    List<String> names = Arrays.stream(header.values()).map(String::trim).toList();
                    if (!names.equals(handle.schema().columns())) {
                        throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "CSV header does not match inferred schema in " + split.object().path());
                    }
                    addBytesWithinPrimary(header.bytesConsumed());
                }
            }
        }

        @Override
        protected RecordReadResult<String[]> readNextRecord() throws IOException {
            CsvRecord record = readCsvRecord();
            if (record == null) {
                return RecordReadResult.finished();
            }

            if (record.blank()) {
                return RecordReadResult.skip(record.bytesConsumed());
            }

            if (record.values().length != handle.schema().columns().size()) {
                throw new TrinoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT, "CSV column count does not match inferred schema in " + split.object().path()
                        + ": expected " + handle.schema().columns().size() + ", got " + record.values().length);
            }
            return RecordReadResult.produce(record.values(), record.bytesConsumed());
        }

        private CsvRecord readCsvRecord() throws IOException {
            long bytesConsumed = 0;
            boolean blank = true;
            List<String> values = new ArrayList<>();
            while (true) {
                Optional<ByteDelimitedRecordReader.Record> lineRecord = readLine();
                if (lineRecord.isEmpty()) {
                    if (parser.isPending()) {
                        throw new IllegalArgumentException("Unterminated CSV record at end of input");
                    }
                    return null;
                }

                ByteDelimitedRecordReader.Record line = lineRecord.orElseThrow();
                bytesConsumed += line.bytesConsumed();
                String value = line.value(charset);
                blank &= value.isBlank();
                String[] parsedValues = handle.options().multiline()
                        ? parser.parseLineMulti(value)
                        : parser.parseLine(value);
                if (parsedValues != null) {
                    Collections.addAll(values, parsedValues);
                }
                if (!parser.isPending()) {
                    return new CsvRecord(values.toArray(String[]::new), bytesConsumed, blank);
                }
            }
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, String[] rawValues) {
            for (int outputIndex = 0; outputIndex < projectedColumns.size(); outputIndex++) {
                S3FileColumnHandle columnHandle = projectedColumns.get(outputIndex);
                int sourceIndex = columnHandle.getOrdinalPosition();
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputIndex);
                String value = sourceIndex < rawValues.length ? rawValues[sourceIndex] : null;
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    ((VarcharType) projectedTypes().get(outputIndex)).writeSlice(blockBuilder, Slices.utf8Slice(value));
                }
            }
            pageBuilder.declarePosition();
        }

        private record CsvRecord(String[] values, long bytesConsumed, boolean blank) {}
    }

}
