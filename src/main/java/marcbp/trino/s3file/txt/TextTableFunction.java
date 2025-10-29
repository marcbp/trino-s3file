package marcbp.trino.s3file.txt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import marcbp.trino.s3file.util.S3ClientBuilder;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import marcbp.trino.s3file.util.CharsetUtils;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that streams plain text files from S3-compatible storage as rows for Trino.
 */
public final class TextTableFunction extends AbstractConnectorTableFunction {
    private static final Logger LOG = LoggerFactory.getLogger(TextTableFunction.class);
    private static final String PATH_ARGUMENT = "PATH";
    private static final String LINE_BREAK_ARGUMENT = "LINE_BREAK";
    private static final String ENCODING_ARGUMENT = "ENCODING";
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;

    public TextTableFunction(S3ClientBuilder s3ClientBuilder) {
        super(
                "txt",
                "load",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name(PATH_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(LINE_BREAK_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("\n"))
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(ENCODING_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(StandardCharsets.UTF_8.name()))
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
        String lineBreak = "\n";
        ScalarArgument lineBreakArg = (ScalarArgument) arguments.get(LINE_BREAK_ARGUMENT);
        if (lineBreakArg != null && lineBreakArg.getValue() instanceof Slice delimiterSlice) {
            lineBreak = decodeEscapes(delimiterSlice.toStringUtf8());
        }
        if (lineBreak.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "LINE_BREAK cannot be empty");
        }

        LOG.info("Analyzing txt.load table function for path {} with line break {}", s3Path, formatForLog(lineBreak));

        long fileSize;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session)) {
            fileSize = s3.getObjectSize(s3Path);
        }

        List<String> columnNames = List.of("line");
        List<Type> columnTypes = List.of(VarcharType.createUnboundedVarcharType());
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, lineBreak, null, fileSize, DEFAULT_SPLIT_SIZE_BYTES, charset.name()))
                .build();
    }

    public TableFunctionProcessorProvider createProcessorProvider() {
        return new ProcessorProvider();
    }

    public List<ConnectorSplit> createSplits(Handle handle) {
        if (handle.getFileSize() == 0) {
            return List.of(new Split("split-0", 0, 0, 0, true, true));
        }
        List<ConnectorSplit> splits = new ArrayList<>();
        long offset = 0;
        int index = 0;
        while (offset < handle.getFileSize()) {
            // The primary region is the portion each worker is responsible for producing.
            long primaryEnd = Math.min(handle.getFileSize(), offset + handle.getSplitSizeBytes());
            long rangeEnd = primaryEnd;
            boolean last = primaryEnd >= handle.getFileSize();
            if (!last) {
                // Extend the HTTP range request a bit beyond the primary region so we can
                // finish reading the trailing line that crosses the split boundary.
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
        if (!(split instanceof Split textSplit)) {
            throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
        }
        return new SplitProcessor(new Processor(session, s3ClientBuilder, handle, textSplit));
    }

    private static String decodeEscapes(String value) {
        StringBuilder result = new StringBuilder();
        boolean escaping = false;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (escaping) {
                switch (ch) {
                    case 'n' -> result.append('\n');
                    case 'r' -> result.append('\r');
                    case 't' -> result.append('\t');
                    case '\\' -> result.append('\\');
                    default -> result.append(ch);
                }
                escaping = false;
            }
            else if (ch == '\\') {
                escaping = true;
            }
            else {
                result.append(ch);
            }
        }
        if (escaping) {
            result.append('\\');
        }
        return result.toString();
    }

    private static String formatForLog(String delimiter) {
        return delimiter
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    public static final class Handle implements ConnectorTableFunctionHandle {
        private final String s3Path;
        private final String lineBreak;
        private final Integer batchSize;
        private final long fileSize;
        private final int splitSizeBytes;
        private final String charsetName;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("lineBreak") String lineBreak,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName) {
            this.s3Path = requireNonNull(s3Path, "s3Path is null");
            this.lineBreak = requireNonNull(lineBreak, "lineBreak is null");
            this.batchSize = batchSize;
            this.fileSize = fileSize;
            this.splitSizeBytes = splitSizeBytes;
            this.charsetName = (charsetName == null || charsetName.isBlank()) ? StandardCharsets.UTF_8.name() : charsetName;
        }

        @JsonProperty
        public String getS3Path() {
            return s3Path;
        }

        @JsonProperty
        public String getLineBreak() {
            return lineBreak;
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
            return List.of(VarcharType.createUnboundedVarcharType());
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
            if (!(handle instanceof Handle textHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            LOG.info("Creating text data processor for path {}", textHandle.getS3Path());
            return new Processor(session, s3ClientBuilder, textHandle, null);
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split) {
            if (!(handle instanceof Handle textHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            if (!(split instanceof Split textSplit)) {
                throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
            }
            return new SplitProcessor(new Processor(session, s3ClientBuilder, textHandle, textSplit));
        }
    }

    private static final class Processor implements TableFunctionDataProcessor {
        private final ConnectorSession session;
        private final S3ClientBuilder.SessionClient sessionClient;
        private final Handle handle;
        private final Split split;
        private final List<Type> columnTypes;
        private final VarcharType outputType;
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
            this.handle = handle;
            this.split = split != null ? split : Split.forWholeFile(handle.getFileSize());
            this.columnTypes = handle.resolveColumnTypes();
            this.outputType = (VarcharType) columnTypes.get(0);
            this.charset = handle.charset();
            this.lineBreakBytes = handle.getLineBreak().getBytes(charset);
            // If the split does not start at byte 0 we skip the first line, because it was already
            //                  terminated by the previous split (or the header read during analysis).
            this.skipFirstLine = this.split.getStartOffset() > 0;
            this.primaryLength = this.split.getPrimaryLength();
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (finished) {
                    LOG.info("Text processor already finished for path {}", handle.getS3Path());
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
                    appendRow(pageBuilder, record.value());
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
                LOG.error("Error while reading text content for path {}", handle.getS3Path(), e);
                closeSession();
                throw new UncheckedIOException("Failed to read text content", e);
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
            LOG.info("Opening text stream for path {} (split {}-{})", handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive());
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

        private void appendRow(PageBuilder pageBuilder, String value) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            outputType.writeSlice(blockBuilder, Slices.utf8Slice(value));
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
                LOG.error("Error closing text stream for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to close text stream", e);
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
