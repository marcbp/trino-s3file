package marcbp.trino.s3file.txt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
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
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;
import static marcbp.trino.s3file.util.TableFunctionArguments.encodingArgumentSpecification;
import static marcbp.trino.s3file.util.TableFunctionArguments.fileArguments;
import static marcbp.trino.s3file.util.TableFunctionArguments.requirePath;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveEncoding;
import static marcbp.trino.s3file.util.TableFunctionArguments.resolveSplitSizeBytes;
import static marcbp.trino.s3file.util.TableFunctionArguments.splitSizeMbArgumentSpecification;

/**
 * Table function that streams plain text files from S3-compatible storage as rows for Trino.
 */
public final class TextTableFunction extends AbstractConnectorTableFunction {
    private static final String LINE_BREAK_ARGUMENT = "LINE_BREAK";
    private final S3ClientBuilder s3ClientBuilder;
    private final int defaultSplitSizeBytes;
    private final PageSettings pageSettings;
    private final Logger logger = Logger.get(TextTableFunction.class);

    public TextTableFunction(S3ClientBuilder s3ClientBuilder) {
        this(s3ClientBuilder, S3ClientConfig.DEFAULT_SPLIT_SIZE_BYTES, PageSettings.DEFAULT);
    }

    public TextTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes) {
        this(s3ClientBuilder, defaultSplitSizeBytes, PageSettings.DEFAULT);
    }

    public TextTableFunction(S3ClientBuilder s3ClientBuilder, int defaultSplitSizeBytes, PageSettings pageSettings) {
        super(
                "txt",
                "load",
                fileArguments(
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

        String lineBreak = "\n";
        ScalarArgument lineBreakArg = (ScalarArgument) arguments.get(LINE_BREAK_ARGUMENT);
        if (lineBreakArg != null && lineBreakArg.getValue() instanceof Slice delimiterSlice) {
            lineBreak = TextFormatSupport.decodeEscapes(delimiterSlice.toStringUtf8());
        }
        if (lineBreak.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "LINE_BREAK cannot be empty");
        }

        logger.info("Analyzing txt.load table function for path %s with line break %s", s3Path, TextFormatSupport.formatForLog(lineBreak));

        FileSelection selection;
        try (S3ClientBuilder.SessionClient s3 = s3ClientBuilder.forSession(session)) {
            selection = FileSelection.resolve(s3, s3Path, arguments);
        }

        Handle handle = new Handle(
                selection,
                new ScanSettings(splitSizeBytes, pageSettings, charset.name()),
                new TextOptions(lineBreak));
        return TableFunctionAnalysis.builder()
                .returnedType(Descriptor.descriptor(handle.columnNames(), handle.resolveColumnTypes()))
                .handle(handle)
                .build();
    }

    public ConnectorPageSource createPageSource(ConnectorSession session, Handle handle, FileSplit split, List<S3FileColumnHandle> columns) {
        return new PageSource(session, s3ClientBuilder, handle, split, columns);
    }

    public record TextOptions(@JsonProperty("lineBreak") String lineBreak) {
        @JsonCreator
        public TextOptions {
            lineBreak = requireNonNull(lineBreak, "lineBreak is null");
        }
    }

    public static final class Handle extends BaseTextFileHandle {
        private final TextOptions options;

        public Handle(
                FileSelection selection,
                ScanSettings scan,
                TextOptions options) {
            super(selection, scan);
            this.options = requireNonNull(options, "options is null");
        }

        @JsonCreator
        public Handle(
                @JsonProperty("pathColumn") String pathColumn,
                @JsonProperty("scan") ScanSettings scan,
                @JsonProperty("options") TextOptions options) {
            super(pathColumn, scan);
            this.options = requireNonNull(options, "options is null");
        }

        @Override
        public String format() {
            return "txt";
        }

        @JsonProperty("options")
        public TextOptions options() {
            return options;
        }

        @Override
        public List<String> dataColumnNames() {
            return List.of("line");
        }

        @Override
        public List<Type> dataColumnTypes() {
            return List.of(VarcharType.createUnboundedVarcharType());
        }
    }

    private static final class PageSource extends AbstractDelimitedFilePageSource<Handle, String> {

        private PageSource(
                ConnectorSession session,
                S3ClientBuilder s3ClientBuilder,
                Handle handle,
                FileSplit split,
                List<S3FileColumnHandle> projectedColumns) {
            super(session, s3ClientBuilder, handle, split, projectedColumns, handle.options().lineBreak(), false);
        }

        @Override
        protected RecordReadResult<String> readNextRecord() throws IOException {
            Optional<ByteDelimitedRecordReader.Record> record = readLine();
            if (record.isEmpty()) {
                return RecordReadResult.finished();
            }
            ByteDelimitedRecordReader.Record textRecord = record.orElseThrow();
            return RecordReadResult.produce(textRecord.value(charset), textRecord.bytesConsumed());
        }

        @Override
        protected void appendRecord(PageBuilder pageBuilder, String payload) {
            if (!projectedColumns.isEmpty()) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
                TextFormatSupport.writeLine(blockBuilder, VarcharType.VARCHAR, payload);
            }
            pageBuilder.declarePosition();
        }
    }
}
