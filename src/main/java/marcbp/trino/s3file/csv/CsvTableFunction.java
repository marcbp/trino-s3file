package marcbp.trino.s3file.csv;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.airlift.log.Logger;
import marcbp.trino.s3file.file.AbstractFileProcessor;
import marcbp.trino.s3file.file.BaseFileHandle;
import marcbp.trino.s3file.file.BaseFileProcessorProvider;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.FileSplitProcessor;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.util.S3ClientBuilder;
import marcbp.trino.s3file.util.CharsetUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.CharsetUtils.resolve;

/**
 * Table function that reads CSV data from S3-compatible storage, inferring columns on the fly.
 */
public final class CsvTableFunction extends AbstractConnectorTableFunction {
    private static final Logger LOG = Logger.get(CsvTableFunction.class);
    private static final String PATH_ARGUMENT = "PATH";
    private static final String DELIMITER_ARGUMENT = "DELIMITER";
    private static final String ENCODING_ARGUMENT = "ENCODING";
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;
    private final CsvProcessingService csvProcessingService;

    public CsvTableFunction(S3ClientBuilder s3ClientBuilder, CsvProcessingService csvProcessingService) {
        super(
                "csv",
                "load",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name(PATH_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .build(),
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
                                .name(ENCODING_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(StandardCharsets.UTF_8.name()))
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
        this.csvProcessingService = requireNonNull(csvProcessingService, "csvProcessingService is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle,
                                         Map<String, Argument> arguments,
                                         io.trino.spi.connector.ConnectorAccessControl accessControl) {
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

        Charset charset = resolve(arguments, ENCODING_ARGUMENT);

        char delimiter = ';';
        ScalarArgument delimiterArg = (ScalarArgument) arguments.get(DELIMITER_ARGUMENT);
        if (delimiterArg != null && delimiterArg.getValue() instanceof Slice delimiterSlice && delimiterSlice.length() > 0) {
            delimiter = (char) delimiterSlice.getByte(0);
        }

        LOG.info("Analyzing load table function for path %s with delimiter %s", s3Path, delimiter);
        boolean headerPresent = true;
        ScalarArgument headerArg = (ScalarArgument) arguments.get("HEADER");
        if (headerArg != null && headerArg.getValue() instanceof Slice headerSlice) {
            String headerText = headerSlice.toStringUtf8();
            LOG.info("HEADER argument value: %s", headerText);
            headerPresent = Boolean.parseBoolean(headerText.trim());
        }
        LOG.info("Header present: %s", headerPresent);

        List<String> columnNames;
        long fileSize;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session);
             BufferedReader reader = s3.openReader(s3Path, charset)) {
            columnNames = csvProcessingService.inferColumnNames(reader, s3Path, delimiter, headerPresent);
            fileSize = s3.getObjectSize(s3Path);
        }
        catch (IOException e) {
            LOG.error(e, "Failed to infer column names for %s", s3Path);
            throw new UncheckedIOException("Failed to infer column names", e);
        }
        LOG.info("Detected %s columns: %s", columnNames.size(), columnNames);
        List<Type> columnTypes = columnNames.stream()
                .map(name -> (Type) VarcharType.createUnboundedVarcharType())
                .toList();
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, columnNames, delimiter, headerPresent, null, fileSize, DEFAULT_SPLIT_SIZE_BYTES, charset.name()))
                .build();
    }

    public TableFunctionProcessorProvider createProcessorProvider() {
        return new ProcessorProvider();
    }

    public List<FileSplit> createSplits(Handle handle) {
        return SplitPlanner.planSplits(handle.getFileSize(), handle.getSplitSizeBytes(), LOOKAHEAD_BYTES);
    }

    public TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
        return new FileSplitProcessor(new Processor(session, s3ClientBuilder, csvProcessingService, handle, split));
    }

    public static final class Handle extends BaseFileHandle {
        private static final int DEFAULT_BATCH_SIZE = 1024;

        private final List<String> columns;
        private final char delimiter;
        private final boolean headerPresent;
        private final Integer batchSize;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("delimiter") char delimiter,
                      @JsonProperty("header") boolean headerPresent,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName) {
            super(s3Path, fileSize, splitSizeBytes, charsetName);
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.delimiter = delimiter;
            this.headerPresent = headerPresent;
            this.batchSize = batchSize;
        }

        @JsonProperty
        public List<String> getColumns() {
            return columns;
        }

        @JsonProperty
        public char getDelimiter() {
            return delimiter;
        }

        @JsonProperty("header")
        public boolean isHeaderPresent() {
            return headerPresent;
        }

        @JsonProperty
        public Integer getBatchSize() {
            return batchSize;
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

    private final class ProcessorProvider extends BaseFileProcessorProvider<Handle> {
        private ProcessorProvider() {
            super(Handle.class);
        }

        @Override
        protected TableFunctionDataProcessor createDataProcessor(ConnectorSession session, Handle handle) {
            LOG.info("Creating data processor for path %s", handle.getS3Path());
            return new Processor(session, s3ClientBuilder, csvProcessingService, handle, null);
        }

        @Override
        protected TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
            return CsvTableFunction.this.createSplitProcessor(session, handle, split);
        }
    }

    private static final class Processor extends AbstractFileProcessor<Handle> {
        private final CsvProcessingService csvProcessingService;
        private final List<Type> columnTypes;
        private final byte[] lineBreakBytes;
        private boolean skipFirstLine;

        private Processor(ConnectorSession session, S3ClientBuilder s3ClientBuilder, CsvProcessingService csvProcessingService, Handle handle, FileSplit split) {
            super(session, s3ClientBuilder, handle, split);
            LOG.debug("Creating processor for path %s", handle.getS3Path());
            this.csvProcessingService = requireNonNull(csvProcessingService, "csvProcessingService is null");
            this.columnTypes = handle.resolveColumnTypes();
            this.lineBreakBytes = "\n".getBytes(charset());
            this.skipFirstLine = split().getStartOffset() > 0;
        }

        @Override
        protected boolean finishWhenEmptySplit() {
            return primaryLength() == 0 && split().getStartOffset() > 0;
        }

        @Override
        protected BufferedReader openReader() {
            if (split().isWholeFile()) {
                return sessionClient().openReader(handle().getS3Path(), charset());
            }
            return sessionClient().openReader(handle().getS3Path(), split().getStartOffset(), split().getRangeEndExclusive(), charset());
        }

        @Override
        protected void afterReaderOpened(BufferedReader reader) throws IOException {
            if (handle().isHeaderPresent() && split().isFirst()) {
                String header = reader.readLine();
                LOG.info("Skipped CSV header for path %s: %s", handle().getS3Path(), header);
                if (header != null) {
                    addBytesWithinPrimary(calculateLineBytes(header));
                }
            }
        }

        @Override
        protected void handleReaderCloseException(IOException e) {
            LOG.error(e, "Error closing CSV stream for path %s", handle().getS3Path());
            throw new UncheckedIOException("Failed to close CSV stream", e);
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (isFinished()) {
                    LOG.info("Processor already finished for path %s", handle().getS3Path());
                    closeSession();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                try {
                    super.ensureReader();
                }
                catch (IOException e) {
                    LOG.error(e, "Unable to read CSV header for path %s", handle().getS3Path());
                    closeSession();
                    throw new UncheckedIOException("Failed to read CSV header", e);
                }
                if (isFinished()) {
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                PageBuilder pageBuilder = new PageBuilder(handle().batchSizeOrDefault(), columnTypes);
                LOG.info("Starting CSV batch read for path %s", handle().getS3Path());
                while (!pageBuilder.isFull()) {
                    String line = reader().readLine();
                    if (line == null) {
                        LOG.info("Reached end of CSV for path %s", handle().getS3Path());
                        completeProcessing();
                        break;
                    }
                    long lineBytes = calculateLineBytes(line);
                    LOG.debug("Read line: %s", line);
                    if (skipFirstLine) {
                        skipFirstLine = false;
                        addBytesWithinPrimary(lineBytes);
                        continue;
                    }
                    if (line.isBlank()) {
                        addBytesWithinPrimary(lineBytes);
                        continue;
                    }
                    String[] values = csvProcessingService.parseCsvLine(line, handle().getDelimiter());
                    LOG.debug("Appending row with %s values for path %s", values.length, handle().getS3Path());
                    boolean finishesSplit = false;
                    if (!split().isLast() && bytesWithinPrimary() + lineBytes > primaryLength()) {
                        finishesSplit = true;
                    }
                    addBytesWithinPrimary(lineBytes);
                    appendRow(pageBuilder, values);
                    if (finishesSplit) {
                        completeProcessing();
                        break;
                    }
                }

                if (pageBuilder.isEmpty()) {
                    LOG.info("No rows produced in this batch for path %s", handle().getS3Path());
                    markFinished();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                LOG.info("Produced %s rows for path %s", page.getPositionCount(), handle().getS3Path());
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error(e, "Error while reading CSV content for path %s", handle().getS3Path());
                closeSession();
                throw new UncheckedIOException("Failed to read CSV content", e);
            }
            catch (RuntimeException e) {
                LOG.error(e, "Unexpected runtime error for path %s", handle().getS3Path());
                closeSession();
                throw e;
            }
        }

        private void appendRow(PageBuilder pageBuilder, String[] rawValues) {
            for (int columnIndex = 0; columnIndex < columnTypes.size(); columnIndex++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnIndex);
                String value = columnIndex < rawValues.length ? rawValues[columnIndex] : null;
                if (value == null) {
                    blockBuilder.appendNull();
                }
                else {
                    ((VarcharType) columnTypes.get(columnIndex)).writeSlice(blockBuilder, Slices.utf8Slice(value));
                }
            }
            pageBuilder.declarePosition();
        }

        private long calculateLineBytes(String value) {
            return value.getBytes(charset()).length + lineBreakBytes.length;
        }
    }

}
