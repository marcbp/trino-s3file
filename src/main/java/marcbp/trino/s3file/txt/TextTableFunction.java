package marcbp.trino.s3file.txt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import marcbp.trino.s3file.file.AbstractFileProcessor;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.s3.S3ClientBuilder;
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

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import marcbp.trino.s3file.file.BaseFileHandle;
import marcbp.trino.s3file.file.BaseFileProcessorProvider;
import marcbp.trino.s3file.file.FileSplitProcessor;
import static java.util.Objects.requireNonNull;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.pathArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;

/**
 * Table function that streams plain text files from S3-compatible storage as rows for Trino.
 */
public final class TextTableFunction extends AbstractConnectorTableFunction {
    private static final String LINE_BREAK_ARGUMENT = "LINE_BREAK";

    private static final int DEFAULT_SPLIT_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;
    private final Logger logger = Logger.get(TextTableFunction.class);

    public TextTableFunction(S3ClientBuilder s3ClientBuilder) {
        super(
                "txt",
                "load",
                List.of(
                        pathArgumentSpecification(),
                        ScalarArgumentSpecification.builder()
                                .name(LINE_BREAK_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice("\n"))
                                .build(),
                        encodingArgumentSpecification()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ClientBuilder = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null");
    }

    @Override
    public TableFunctionAnalysis analyze(ConnectorSession session,
                                         ConnectorTransactionHandle transactionHandle,
                                         Map<String, Argument> arguments,
                                         ConnectorAccessControl accessControl) {
        String s3Path = requirePath(arguments);
        Charset charset = resolveEncoding(arguments);
        
        String lineBreak = "\n";
        ScalarArgument lineBreakArg = (ScalarArgument) arguments.get(LINE_BREAK_ARGUMENT);
        if (lineBreakArg != null && lineBreakArg.getValue() instanceof Slice delimiterSlice) {
            lineBreak = TextFormatSupport.decodeEscapes(delimiterSlice.toStringUtf8());
        }
        if (lineBreak.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "LINE_BREAK cannot be empty");
        }

        logger.info("Analyzing txt.load table function for path %s with line break %s", s3Path, TextFormatSupport.formatForLog(lineBreak));

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

    public static final class Handle extends BaseFileHandle {
        private final String lineBreak;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("lineBreak") String lineBreak,
                      @JsonProperty("batchSize") Integer batchSize,
                      @JsonProperty("fileSize") long fileSize,
                      @JsonProperty("splitSizeBytes") int splitSizeBytes,
                      @JsonProperty("charset") String charsetName) {
            super(s3Path, fileSize, splitSizeBytes, charsetName, batchSize == null ? BaseFileHandle.DEFAULT_BATCH_SIZE : batchSize);
            this.lineBreak = requireNonNull(lineBreak, "lineBreak is null");
        }

        @JsonProperty
        public String getLineBreak() {
            return lineBreak;
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
            this.lineBreakBytes = handle.getLineBreak().getBytes(charset);
        }

        @Override
        protected List<Type> columnTypes() {
            return columnTypes;
        }

        @Override
        protected RecordReadResult<?> readNextRecord() throws IOException {
            Optional<TextFormatSupport.TextRecord> record = TextFormatSupport.readNextLine(
                    reader(),
                    charset,
                    lineBreakBytes,
                    primaryLength,
                    bytesWithinPrimary,
                    split.isLast());
            if (record.isEmpty()) {
                return RecordReadResult.finished();
            }
            TextFormatSupport.TextRecord textRecord = record.get();
            return RecordReadResult.produce(textRecord.value(), textRecord.bytes(), textRecord.finishesSplit());
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object payload) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
            TextFormatSupport.writeLine(blockBuilder, outputType, (String) payload);
            pageBuilder.declarePosition();
        }

    }
}
