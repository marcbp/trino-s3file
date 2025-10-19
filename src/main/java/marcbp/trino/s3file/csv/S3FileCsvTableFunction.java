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
import io.trino.spi.connector.ConnectorSplit;
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
import marcbp.trino.s3file.S3ObjectService;
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
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that reads CSV data from S3-compatible storage, inferring columns on the fly.
 */
public final class S3FileCsvTableFunction extends AbstractConnectorTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileCsvTableFunction.class);
    private static final String PATH_ARGUMENT = "PATH";
    private static final String DELIMITER_ARGUMENT = "DELIMITER";
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ObjectService s3ObjectService;
    private final CsvProcessingService csvProcessingService;

    public S3FileCsvTableFunction(S3ObjectService s3ObjectService, CsvProcessingService csvProcessingService) {
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
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ObjectService = requireNonNull(s3ObjectService, "s3ObjectService is null");
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

        char delimiter = ';';
        ScalarArgument delimiterArg = (ScalarArgument) arguments.get(DELIMITER_ARGUMENT);
        if (delimiterArg != null && delimiterArg.getValue() instanceof Slice delimiterSlice && delimiterSlice.length() > 0) {
            delimiter = (char) delimiterSlice.getByte(0);
        }

        LOG.info("Analyzing load table function for path {} with delimiter {}", s3Path, delimiter);
        boolean headerPresent = true;
        ScalarArgument headerArg = (ScalarArgument) arguments.get("HEADER");
        if (headerArg != null && headerArg.getValue() instanceof Slice headerSlice) {
            String headerText = headerSlice.toStringUtf8();
            LOG.info("HEADER argument value: {}", headerText);
            headerPresent = Boolean.parseBoolean(headerText.trim());
        }
        LOG.info("Header present: {}", headerPresent);

        List<String> columnNames;
        try (BufferedReader reader = s3ObjectService.openReader(s3Path)) {
            columnNames = csvProcessingService.inferColumnNames(reader, s3Path, delimiter, headerPresent);
        }
        catch (IOException e) {
            LOG.error("Failed to infer column names for {}", s3Path, e);
            throw new UncheckedIOException("Failed to infer column names", e);
        }
        LOG.info("Detected {} columns: {}", columnNames.size(), columnNames);
        List<Type> columnTypes = columnNames.stream()
                .map(name -> (Type) VarcharType.createUnboundedVarcharType())
                .toList();
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        long fileSize = s3ObjectService.getObjectSize(s3Path);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, columnNames, delimiter, headerPresent, null, fileSize, DEFAULT_SPLIT_SIZE_BYTES))
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
            boolean last = primaryEnd >= handle.getFileSize();
            long rangeEnd = last ? handle.getFileSize() : Math.min(handle.getFileSize(), primaryEnd + LOOKAHEAD_BYTES);
            splits.add(new Split(
                    "split-" + index,
                    offset,
                    primaryEnd,
                    rangeEnd,
                    offset == 0,
                    last));
            offset = primaryEnd;
            index++;
        }
        return splits;
    }

    public TableFunctionSplitProcessor createSplitProcessor(Handle handle, ConnectorSplit split) {
        if (!(split instanceof Split csvSplit)) {
            throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
        }
        return new SplitProcessor(new Processor(s3ObjectService, csvProcessingService, handle, csvSplit));
    }

    public static final class Handle implements ConnectorTableFunctionHandle {
        private static final int DEFAULT_BATCH_SIZE = 1024;

        private final String s3Path;
        private final List<String> columns;
        private final char delimiter;
        private final boolean headerPresent;
        private final Integer batchSize;
        private final long fileSize;
        private final int splitSizeBytes;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("delimiter") char delimiter,
                      @JsonProperty("header") boolean headerPresent,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes) {
            this.s3Path = requireNonNull(s3Path, "s3Path is null");
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.delimiter = delimiter;
            this.headerPresent = headerPresent;
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
            if (!(handle instanceof Handle csvHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            LOG.info("Creating data processor for path {}", csvHandle.getS3Path());
            return new Processor(s3ObjectService, csvProcessingService, csvHandle, null);
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle functionHandle, ConnectorSplit split) {
            if (!(functionHandle instanceof Handle csvHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
            }
            if (!(split instanceof Split csvSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return createSplitProcessor(csvHandle, csvSplit);
        }
    }

    private static final class Processor implements TableFunctionDataProcessor {
        private final S3ObjectService s3ObjectService;
        private final CsvProcessingService csvProcessingService;
        private final Handle handle;
        private final List<Type> columnTypes;
        private final Split split;
        private final long primaryLength;
        private final Charset charset = StandardCharsets.UTF_8;
        private final byte[] lineBreakBytes = "\n".getBytes(StandardCharsets.UTF_8);

        private BufferedReader reader;
        private boolean finished;
        private boolean skipFirstLine;
        private long bytesWithinPrimary;

        private Processor(S3ObjectService s3ObjectService, CsvProcessingService csvProcessingService, Handle handle, Split split) {
            LOG.debug("Creating processor for path {}", handle.getS3Path());
            this.s3ObjectService = s3ObjectService;
            this.csvProcessingService = csvProcessingService;
            this.handle = handle;
            this.columnTypes = handle.resolveColumnTypes();
            this.split = split != null ? split : Split.forWholeFile(handle.getFileSize());
            this.primaryLength = this.split.getPrimaryLength();
            this.skipFirstLine = this.split.getStartOffset() > 0;
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (finished) {
                    LOG.info("Processor already finished for path {}", handle.getS3Path());
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                ensureReader();
                if (finished) {
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                PageBuilder pageBuilder = new PageBuilder(handle.batchSizeOrDefault(), columnTypes);
                LOG.info("Starting CSV batch read for path {}", handle.getS3Path());
                while (!pageBuilder.isFull()) {
                    String line = reader.readLine();
                    if (line == null) {
                        LOG.info("Reached end of CSV for path {}", handle.getS3Path());
                        completeProcessing();
                        break;
                    }
                    long lineBytes = calculateLineBytes(line);
                    LOG.debug("Read line: {}", line);
                    if (skipFirstLine) {
                        skipFirstLine = false;
                        bytesWithinPrimary += lineBytes;
                        continue;
                    }
                    if (line.isBlank()) {
                        bytesWithinPrimary += lineBytes;
                        continue;
                    }
                    String[] values = csvProcessingService.parseCsvLine(line, handle.getDelimiter());
                    LOG.debug("Appending row with {} values for path {}", values.length, handle.getS3Path());
                    boolean finishesSplit = false;
                    if (!split.isLast() && bytesWithinPrimary + lineBytes > primaryLength) {
                        finishesSplit = true;
                    }
                    bytesWithinPrimary += lineBytes;
                    appendRow(pageBuilder, values);
                    if (finishesSplit) {
                        completeProcessing();
                        break;
                    }
                }

                if (pageBuilder.isEmpty()) {
                    LOG.info("No rows produced in this batch for path {}", handle.getS3Path());
                    finished = true;
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                LOG.info("Produced {} rows for path {}", page.getPositionCount(), handle.getS3Path());
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error("Error while reading CSV content for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to read CSV content", e);
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
            if (primaryLength == 0 && split.getStartOffset() > 0) {
                finished = true;
                return;
            }
            LOG.info("Opening CSV stream for path {}", handle.getS3Path());
            if (split.isWholeFile()) {
                reader = s3ObjectService.openReader(handle.getS3Path());
            }
            else {
                reader = s3ObjectService.openReader(handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive());
            }
            try {
                if (handle.isHeaderPresent() && split.isFirst()) {
                    String header = reader.readLine();
                    LOG.info("Skipped CSV header for path {}: {}", handle.getS3Path(), header);
                    if (header != null) {
                        bytesWithinPrimary += calculateLineBytes(header);
                    }
                }
            }
            catch (IOException e) {
                LOG.error("Unable to read CSV header for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to read CSV header", e);
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
                LOG.error("Error closing CSV stream for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to close CSV stream", e);
            }
            finally {
                reader = null;
                finished = true;
            }
        }

        private long calculateLineBytes(String value) {
            return value.getBytes(charset).length + lineBreakBytes.length;
        }
    }

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

        private boolean isWholeFile() {
            return first && last && startOffset == 0 && rangeEndExclusive == primaryEndOffset;
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
