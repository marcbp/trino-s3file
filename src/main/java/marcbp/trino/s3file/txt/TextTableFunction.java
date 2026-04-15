package marcbp.trino.s3file.txt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import marcbp.trino.s3file.file.FileSplit;
import marcbp.trino.s3file.file.TextSplitBoundarySupport;
import marcbp.trino.s3file.file.SplitPlanner;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
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
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import marcbp.trino.s3file.file.BaseTextFileHandle;
import marcbp.trino.s3file.file.AbstractTextFilePageSource;
import static java.util.Objects.requireNonNull;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.pathArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveSplitSizeBytes;
import static marcbp.trino.s3file.util.TableFunctionArguments.splitSizeMbArgumentSpecification;

/**
 * Table function that streams plain text files from S3-compatible storage as rows for Trino.
 */
public final class TextTableFunction extends AbstractConnectorTableFunction {
    private static final String LINE_BREAK_ARGUMENT = "LINE_BREAK";
    private static final int LOOKAHEAD_BYTES = 256 * 1024;

    private final S3ClientBuilder s3ClientBuilder;
    private final int defaultSplitSizeBytes;
    private final Logger logger = Logger.get(TextTableFunction.class);

    public TextTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, marcbp.trino.s3file.s3.S3ClientConfig.DEFAULT_SPLIT_SIZE_BYTES);
    }

    public TextTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes) {
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
                                         ConnectorAccessControl accessControl) {
        String s3Path = requirePath(arguments);
        Charset charset = resolveEncoding(arguments);
        int splitSizeBytes = resolveSplitSizeBytes(arguments, defaultSplitSizeBytes);
        long analyzeStartedAt = System.nanoTime();
        
        String lineBreak = "\n";
        ScalarArgument lineBreakArg = (ScalarArgument) arguments.get(LINE_BREAK_ARGUMENT);
        if (lineBreakArg != null && lineBreakArg.getValue() instanceof Slice delimiterSlice) {
            lineBreak = TextFormatSupport.decodeEscapes(delimiterSlice.toStringUtf8());
        }
        if (lineBreak.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "LINE_BREAK cannot be empty");
        }

        logger.info("Analyzing txt.load table function for path %s with line break %s", s3Path, TextFormatSupport.formatForLog(lineBreak));

        S3ClientBuilder.ObjectMetadata metadata;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session)) {
            metadata = s3.getObjectMetadata(s3Path);
        }

        List<String> columnNames = List.of("line");
        List<Type> columnTypes = List.of(VarcharType.createUnboundedVarcharType());
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(
                        s3Path,
                        lineBreak,
                        null,
                        metadata.size(),
                        splitSizeBytes,
                        charset.name(),
                        metadata.eTag().orElse(null),
                        metadata.versionId().orElse(null),
                        0L,
                        1,
                        System.nanoTime() - analyzeStartedAt))
                .build();
    }

    public List<FileSplit> createSplits(Handle handle) {
        return SplitPlanner.planSplits(handle.getFileSize(), handle.getSplitSizeBytes(), LOOKAHEAD_BYTES);
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, FileSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, split, columns);
    }

    public static final class Handle extends BaseTextFileHandle {
        private final String lineBreak;

        public Handle(
                String s3Path,
                String lineBreak,
                Integer batchSize,
                long fileSize,
                int splitSizeBytes,
                String charsetName,
                String eTag,
                String versionId) {
            this(s3Path, lineBreak, batchSize, fileSize, splitSizeBytes, charsetName, eTag, versionId, 0L, 0, 0L);
        }

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("lineBreak") String lineBreak,
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
            this.lineBreak = requireNonNull(lineBreak, "lineBreak is null");
        }

        @Override
        public String format() {
            return "txt";
        }

        @JsonProperty
        public String getLineBreak() {
            return lineBreak;
        }

        @Override
        public List<String> columnNames() {
            return List.of("line");
        }

        @Override
        public List<Type> resolveColumnTypes() {
            return List.of(VarcharType.createUnboundedVarcharType());
        }
    }

    private static final class PageSource extends AbstractTextFilePageSource<Handle> {
        private final VarcharType outputType;
        private final String lineBreak;
        private final byte[] lineBreakBytes;
        private boolean skipFirstRecord;

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns);
            this.outputType = VarcharType.createUnboundedVarcharType();
            this.lineBreak = handle.getLineBreak();
            this.lineBreakBytes = lineBreak.getBytes(charset);
            this.skipFirstRecord = split.getStartOffset() > 0;
        }

        @Override
        protected boolean finishWhenEmptySplit() {
            return primaryLength == 0 && split.getStartOffset() > 0;
        }

        @Override
        protected void afterReaderOpened(BufferedReader reader) throws IOException {
            if (skipFirstRecord) {
                recordS3Request();
                skipFirstRecord = !TextSplitBoundarySupport.startsAfterDelimiter(sessionClient, handle, split, lineBreakBytes);
            }
        }

        @Override
        protected RecordReadResult<?> readNextRecord() throws IOException {
            Optional<TextFormatSupport.TextRecord> record = TextFormatSupport.readNextLine(
                    reader(),
                    charset,
                    lineBreak,
                    lineBreakBytes,
                    primaryLength,
                    bytesWithinPrimary,
                    split.isLast());
            if (record.isEmpty()) {
                return RecordReadResult.finished();
            }
            TextFormatSupport.TextRecord textRecord = record.get();
            if (skipFirstRecord) {
                skipFirstRecord = false;
                return RecordReadResult.skip(textRecord.bytes());
            }
            return RecordReadResult.produce(textRecord.value(), textRecord.bytes(), textRecord.finishesSplit());
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, Object payload) {
            if (!projectedColumns.isEmpty()) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
                TextFormatSupport.writeLine(blockBuilder, outputType, (String) payload);
            }
            pageBuilder.declarePosition();
        }
    }
}
