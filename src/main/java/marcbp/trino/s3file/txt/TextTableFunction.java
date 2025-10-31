package marcbp.trino.s3file.txt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import marcbp.trino.s3file.file.AbstractFileProcessor;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.util.S3ClientBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorAccessControl;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import marcbp.trino.s3file.file.BaseFileHandle;
import marcbp.trino.s3file.file.BaseFileProcessorProvider;
import marcbp.trino.s3file.file.FileSplitProcessor;
import marcbp.trino.s3file.util.CharsetUtils;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

/**
 * Table function that streams plain text files from S3-compatible storage as rows for Trino.
 */
public final class TextTableFunction extends AbstractConnectorTableFunction {
    private static final Logger LOG = Logger.get(TextTableFunction.class);
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

        LOG.info("Analyzing txt.load table function for path %s with line break %s", s3Path, formatForLog(lineBreak));

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

    public List<FileSplit> createSplits(Handle handle) {
        return SplitPlanner.planSplits(handle.getFileSize(), handle.getSplitSizeBytes(), LOOKAHEAD_BYTES);
    }

    public TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
        return new FileSplitProcessor(new Processor(session, s3ClientBuilder, handle, split));
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

    public static final class Handle extends BaseFileHandle {
        private final String lineBreak;
        private final Integer batchSize;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("lineBreak") String lineBreak,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName) {
            super(s3Path, fileSize, splitSizeBytes, charsetName);
            this.lineBreak = requireNonNull(lineBreak, "lineBreak is null");
            this.batchSize = batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
        }

        @JsonProperty
        public String getLineBreak() {
            return lineBreak;
        }

        @JsonProperty
        public Integer getBatchSize() {
            return batchSize;
        }

        public List<Type> resolveColumnTypes() {
            return List.of(VarcharType.createUnboundedVarcharType());
        }
    }

    private final class ProcessorProvider extends BaseFileProcessorProvider<Handle> {
        private ProcessorProvider() {
            super(Handle.class);
        }

        @Override
        protected TableFunctionDataProcessor createDataProcessor(ConnectorSession session, Handle handle) {
            return new Processor(session, s3ClientBuilder, handle, null);
        }

        @Override
        protected TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, Handle handle, FileSplit split) {
            return new FileSplitProcessor(new Processor(session, s3ClientBuilder, handle, split));
        }
    }

    private static final class Processor extends AbstractFileProcessor<Handle> {
        private final List<Type> columnTypes;
        private final VarcharType outputType;
        private final byte[] lineBreakBytes;

        private Processor(ConnectorSession session, S3ClientBuilder s3ClientBuilder, Handle handle, FileSplit split) {
            super(session, s3ClientBuilder, handle, split);
            this.columnTypes = handle.resolveColumnTypes();
            this.outputType = (VarcharType) columnTypes.get(0);
            this.lineBreakBytes = handle.getLineBreak().getBytes(charset());
        }

        @Override
        protected BufferedReader openReader() {
            return sessionClient().openReader(handle().getS3Path(), split().getStartOffset(), split().getRangeEndExclusive(), charset());
        }

        @Override
        protected void handleReaderCloseException(IOException e) {
            LOG.error(e, "Error closing text stream for path %s", handle().getS3Path());
            throw new UncheckedIOException("Failed to close text stream", e);
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (isFinished()) {
                    LOG.info("Text processor already finished for path %s", handle().getS3Path());
                    closeSession();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                if (primaryLength() == 0) {
                    markFinished();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                ensureReader();
                if (isFinished()) {
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                PageBuilder pageBuilder = new PageBuilder(handle().getBatchSize(), columnTypes);
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
                    markFinished();
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error(e, "Error while reading text content for path %s", handle().getS3Path());
                closeSession();
                throw new UncheckedIOException("Failed to read text content", e);
            }
            catch (RuntimeException e) {
                LOG.error(e, "Unexpected runtime error for path %s", handle().getS3Path());
                closeSession();
                throw e;
            }
        }

        private LineRead nextRecord() throws IOException {
            while (true) {
                String line = reader().readLine();
                if (line == null) {
                    return null;
                }
                long lineBytes = calculateLineBytes(line);
                boolean finishesSplit = false;
                if (!split().isLast() && bytesWithinPrimary() + lineBytes > primaryLength()) {
                    finishesSplit = true;
                }
                addBytesWithinPrimary(lineBytes);
                return new LineRead(line, finishesSplit);
            }
        }

        private void appendRow(PageBuilder pageBuilder, String value) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            outputType.writeSlice(blockBuilder, Slices.utf8Slice(value));
            pageBuilder.declarePosition();
        }

        private long calculateLineBytes(String value) {
            return value.getBytes(charset()).length + lineBreakBytes.length;
        }
    }

    private record LineRead(String value, boolean finishesSplit) {}
}
