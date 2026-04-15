package marcbp.trino.s3file.csv;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.opencsv.CSVParser;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.airlift.log.Logger;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.file.AbstractTextFilePageSource;
import marcbp.trino.s3file.file.AnalysisStats;
import marcbp.trino.s3file.file.BaseTextFileHandle;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.S3ObjectRef;
import marcbp.trino.s3file.file.ScanSettings;
import marcbp.trino.s3file.file.TextSplitBoundarySupport;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.pathArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveSplitSizeBytes;
import static marcbp.trino.s3file.util.TableFunctionArguments.splitSizeMbArgumentSpecification;

/**
 * Table function that reads CSV data from S3-compatible storage, inferring columns on the fly.
 */
public final class CsvTableFunction extends AbstractConnectorTableFunction {    
    private static final String DELIMITER_ARGUMENT = "DELIMITER";
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;
    private final int defaultSplitSizeBytes;
    private final Logger logger = Logger.get(CsvTableFunction.class);

    public CsvTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, marcbp.trino.s3file.s3.S3ClientConfig.DEFAULT_SPLIT_SIZE_BYTES);
    }

    public CsvTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes) {
        super(
                "csv",
                "load",
                List.of(
                        pathArgumentSpecification(),
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
                        encodingArgumentSpecification(),
                        splitSizeMbArgumentSpecification()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
        this.defaultSplitSizeBytes = defaultSplitSizeBytes;
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle,
                                         Map<String, Argument> arguments,
                                         io.trino.spi.connector.ConnectorAccessControl accessControl) {
        String s3Path = requirePath(arguments);
        Charset charset = resolveEncoding(arguments);
        int splitSizeBytes = resolveSplitSizeBytes(arguments, defaultSplitSizeBytes);
        long analyzeStartedAt = System.nanoTime();

        char delimiter = ';';
        ScalarArgument delimiterArg = (ScalarArgument) arguments.get(DELIMITER_ARGUMENT);
        if (delimiterArg != null && delimiterArg.getValue() instanceof Slice delimiterSlice && delimiterSlice.length() > 0) {
            delimiter = (char) delimiterSlice.getByte(0);
        }

        logger.info("Analyzing load table function for path %s with delimiter %s", s3Path, delimiter);
        boolean headerPresent = true;
        ScalarArgument headerArg = (ScalarArgument) arguments.get("HEADER");
        if (headerArg != null && headerArg.getValue() instanceof Slice headerSlice) {
            String headerText = headerSlice.toStringUtf8();
            headerPresent = Boolean.parseBoolean(headerText.trim());
        }
        logger.info("Header present: %s", headerPresent);

        List<String> columnNames;
        S3ClientBuilder.ObjectMetadata metadata;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session);
             BufferedReader reader = s3.openReader(s3Path, charset)) {
            metadata = s3.getObjectMetadata(s3Path);
            columnNames = CsvFormatSupport.inferColumnNames(reader, s3Path, delimiter, headerPresent);
        }
        catch (IOException e) {
            logger.error(e, "Failed to infer column names for %s", s3Path);
            throw new UncheckedIOException("Failed to infer column names", e);
        }
        logger.info("Detected %s columns: %s", columnNames.size(), columnNames);
        List<Type> columnTypes = columnNames.stream()
                .map(name -> (Type) VarcharType.createUnboundedVarcharType())
                .toList();
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(
                        new S3ObjectRef(s3Path, metadata.size(), metadata.eTag().orElse(null), metadata.versionId().orElse(null)),
                        new ScanSettings(splitSizeBytes, BaseTextFileHandle.DEFAULT_BATCH_SIZE, charset.name()),
                        new AnalysisStats(1L, columnNames.size(), System.nanoTime() - analyzeStartedAt),
                        new CsvSchema(columnNames),
                        new CsvOptions(delimiter, headerPresent)))
                .build();
    }

    public List<FileSplit> createSplits(Handle handle) {
        return SplitPlanner.planSplits(handle.object().size(), handle.scan().splitSizeBytes(), LOOKAHEAD_BYTES);
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
            @JsonProperty("header") boolean headerPresent) {
        @JsonCreator
        public CsvOptions {}
    }

    public static final class Handle extends BaseTextFileHandle {
        private final CsvSchema schema;
        private final CsvOptions options;

        @JsonCreator
        public Handle(
                @JsonProperty("object") S3ObjectRef object,
                @JsonProperty("scan") ScanSettings scan,
                @JsonProperty("analysis") AnalysisStats analysis,
                @JsonProperty("schema") CsvSchema schema,
                @JsonProperty("options") CsvOptions options) {
            super(object, scan, analysis);
            this.schema = requireNonNull(schema, "schema is null");
            this.options = requireNonNull(options, "options is null");
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
        public List<String> columnNames() {
            return schema.columns();
        }

        @Override
        public List<Type> resolveColumnTypes() {
            List<Type> types = new ArrayList<>(schema.columns().size());
            for (int i = 0; i < schema.columns().size(); i++) {
                types.add(VarcharType.createUnboundedVarcharType());
            }
            return List.copyOf(types);
        }
    }

    private static final class PageSource extends AbstractTextFilePageSource<Handle> {
        private final byte[] lineBreakBytes;
        private final CSVParser parser;
        private boolean skipFirstLine;

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns);
            this.lineBreakBytes = "\n".getBytes(charset);
            this.parser = CsvFormatSupport.newParser(handle.options().delimiter());
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
            if (handle.options().headerPresent() && split.isFirst()) {
                String header = reader.readLine();
                if (header != null) {
                    addBytesWithinPrimary(CsvFormatSupport.calculateLineBytes(header, charset, lineBreakBytes));
                }
            }
        }

        @Override
        protected RecordReadResult<?> readNextRecord() throws IOException {
            String line = reader().readLine();
            if (line == null) {
                return RecordReadResult.finished();
            }

            long lineBytes = CsvFormatSupport.calculateLineBytes(line, charset, lineBreakBytes);
            if (skipFirstLine) {
                skipFirstLine = false;
                return RecordReadResult.skip(lineBytes);
            }
            if (line.isBlank()) {
                return RecordReadResult.skip(lineBytes);
            }

            String[] values = CsvFormatSupport.parseCsvLine(line, parser);
            boolean finishesSplit = !split.isLast() && bytesWithinPrimary + lineBytes > primaryLength;
            return RecordReadResult.produce(values, lineBytes, finishesSplit);
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object payload) {
            String[] rawValues = (String[]) payload;
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
    }

}
