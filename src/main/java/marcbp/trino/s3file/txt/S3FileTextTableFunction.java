package marcbp.trino.s3file.txt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import marcbp.trino.s3file.S3FileLogger;
import marcbp.trino.s3file.S3ObjectService;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

public final class S3FileTextTableFunction extends AbstractConnectorTableFunction {
    private static final S3FileLogger LOG = S3FileLogger.get(S3FileTextTableFunction.class);
    private static final String PATH_ARGUMENT = "PATH";
    private static final String LINE_BREAK_ARGUMENT = "LINE_BREAK";
    private static final int DEFAULT_BATCH_SIZE = 1024;
    private static final int BUFFER_SIZE = 4096;

    private final S3ObjectService s3ObjectService;

    public S3FileTextTableFunction(S3ObjectService s3ObjectService) {
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
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.s3ObjectService = requireNonNull(s3ObjectService, "s3ObjectService is null");
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

        String lineBreak = "\n";
        ScalarArgument lineBreakArg = (ScalarArgument) arguments.get(LINE_BREAK_ARGUMENT);
        if (lineBreakArg != null && lineBreakArg.getValue() instanceof Slice delimiterSlice) {
            lineBreak = decodeEscapes(delimiterSlice.toStringUtf8());
        }
        if (lineBreak.isEmpty()) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "LINE_BREAK cannot be empty");
        }

        LOG.info("Analyzing txt.load table function for path {} with line break {}", s3Path, formatForLog(lineBreak));

        List<String> columnNames = List.of("line");
        List<Type> columnTypes = List.of(VarcharType.createUnboundedVarcharType());
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, lineBreak, null))
                .build();
    }

    public TableFunctionProcessorProvider createProcessorProvider() {
        return new ProcessorProvider(s3ObjectService);
    }

    public ConnectorSplit createSplit() {
        return new Split();
    }

    public TableFunctionSplitProcessor createSplitProcessor(Handle handle) {
        return new SplitProcessor(new Processor(s3ObjectService, handle));
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

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("lineBreak") String lineBreak,
                      @JsonProperty("batchSize") Integer batchSize) {
            this.s3Path = requireNonNull(s3Path, "s3Path is null");
            this.lineBreak = requireNonNull(lineBreak, "lineBreak is null");
            this.batchSize = batchSize;
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

        public List<Type> resolveColumnTypes() {
            return List.of(VarcharType.createUnboundedVarcharType());
        }

        public int batchSizeOrDefault() {
            return batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
        }
    }

    private static final class ProcessorProvider implements TableFunctionProcessorProvider {
        private final S3ObjectService s3ObjectService;

        private ProcessorProvider(S3ObjectService s3ObjectService) {
            this.s3ObjectService = s3ObjectService;
        }

        @Override
        public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle) {
            if (!(handle instanceof Handle textHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            LOG.info("Creating text data processor for path {}", textHandle.getS3Path());
            return new Processor(s3ObjectService, textHandle);
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle, ConnectorSplit split) {
            if (!(handle instanceof Handle textHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            return new SplitProcessor(new Processor(s3ObjectService, textHandle));
        }
    }

    private static final class Processor implements TableFunctionDataProcessor {
        private final S3ObjectService s3ObjectService;
        private final Handle handle;
        private final List<Type> columnTypes;
        private final VarcharType outputType;
        private final StringBuilder buffer = new StringBuilder();
        private final char[] chunk = new char[BUFFER_SIZE];
        private BufferedReader reader;
        private boolean finished;
        private boolean endOfFile;

        private Processor(S3ObjectService s3ObjectService, Handle handle) {
            this.s3ObjectService = s3ObjectService;
            this.handle = handle;
            this.columnTypes = handle.resolveColumnTypes();
            this.outputType = (VarcharType) columnTypes.get(0);
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (finished) {
                    LOG.info("Text processor already finished for path {}", handle.getS3Path());
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                ensureReader();
                PageBuilder pageBuilder = new PageBuilder(handle.batchSizeOrDefault(), columnTypes);
                while (!pageBuilder.isFull()) {
                    String record = nextRecord();
                    if (record == null) {
                        completeProcessing();
                        break;
                    }
                    appendRow(pageBuilder, record);
                }

                if (pageBuilder.isEmpty()) {
                    finished = true;
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                Page page = pageBuilder.build();
                return TableFunctionProcessorState.Processed.produced(page);
            }
            catch (IOException e) {
                LOG.error("Error while reading text content for path {}", handle.getS3Path(), e);
                throw new UncheckedIOException("Failed to read text content", e);
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
            LOG.info("Opening text stream for path {}", handle.getS3Path());
            reader = s3ObjectService.openReader(handle.getS3Path());
        }

        private String nextRecord() throws IOException {
            while (true) {
                int index = buffer.indexOf(handle.getLineBreak());
                if (index >= 0) {
                    String record = buffer.substring(0, index);
                    buffer.delete(0, index + handle.getLineBreak().length());
                    return record;
                }
                if (endOfFile) {
                    if (buffer.length() == 0) {
                        return null;
                    }
                    String record = buffer.toString();
                    buffer.setLength(0);
                    return record;
                }
                int read = reader.read(chunk);
                if (read == -1) {
                    endOfFile = true;
                    continue;
                }
                buffer.append(chunk, 0, read);
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
    }

    public static final class Split implements ConnectorSplit {
        private final String id;

        @JsonCreator
        public Split(@JsonProperty("id") String id) {
            this.id = requireNonNull(id, "id is null");
        }

        public Split() {
            this("singleton");
        }

        @JsonProperty
        public String getId() {
            return id;
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
