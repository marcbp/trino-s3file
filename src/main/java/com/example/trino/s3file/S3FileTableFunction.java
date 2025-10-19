package com.example.trino.s3file;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.example.trino.s3file.S3FileLogger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ReturnTypeSpecification;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.spi.connector.ConnectorSession;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.util.Objects.requireNonNull;

public final class S3FileTableFunction extends AbstractConnectorTableFunction {
    private static final S3FileLogger LOG = S3FileLogger.get(S3FileTableFunction.class);
    private static final String PATH_ARGUMENT = "PATH";
    private static final String DELIMITER_ARGUMENT = "DELIMITER";

    private final S3CsvService csvService;

    public S3FileTableFunction(S3CsvService csvService) {
        super(
                "system",
                "test",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name(PATH_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name(DELIMITER_ARGUMENT)
                                .type(VarcharType.VARCHAR)
                                .defaultValue(Slices.utf8Slice(";"))
                                .build()
                ),
                ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
        this.csvService = requireNonNull(csvService, "csvService is null");
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

        LOG.info("Analyzing CSV table function for path {} with delimiter {}", s3Path, delimiter);
        List<String> columnNames = csvService.inferColumnNames(s3Path, delimiter);
        LOG.info("Detected {} columns: {}", columnNames.size(), columnNames);
        List<Type> columnTypes = columnNames.stream()
                .map(name -> (Type) VarcharType.createUnboundedVarcharType())
                .toList();
        Descriptor descriptor = Descriptor.descriptor(columnNames, columnTypes);

        return TableFunctionAnalysis.builder()
                .returnedType(descriptor)
                .handle(new Handle(s3Path, columnNames, delimiter, null))
                .build();
    }

    public TableFunctionProcessorProvider createProcessorProvider() {
        return new ProcessorProvider(csvService);
    }

    public ConnectorSplit createSplit() {
        return new Split();
    }

    public TableFunctionSplitProcessor createSplitProcessor(Handle handle) {
        return new SplitProcessor(new Processor(csvService, handle));
    }

    public static final class Handle implements ConnectorTableFunctionHandle {
        private static final int DEFAULT_BATCH_SIZE = 1024;

        private final String s3Path;
        private final List<String> columns;
        private final char delimiter;
        private final Integer batchSize;

        @JsonCreator
        public Handle(@JsonProperty("s3Path") String s3Path,
                      @JsonProperty("columns") List<String> columns,
                      @JsonProperty("delimiter") char delimiter,
                      @JsonProperty("batchSize") Integer batchSize) {
            this.s3Path = requireNonNull(s3Path, "s3Path is null");
            this.columns = List.copyOf(requireNonNull(columns, "columns is null"));
            this.delimiter = delimiter;
            this.batchSize = batchSize;
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

        @JsonProperty
        public Integer getBatchSize() {
            return batchSize;
        }

        public List<Type> resolveColumnTypes() {
            List<Type> types = new java.util.ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                types.add(VarcharType.createUnboundedVarcharType());
            }
            return java.util.List.copyOf(types);
        }

        public int batchSizeOrDefault() {
            return batchSize == null ? DEFAULT_BATCH_SIZE : batchSize;
        }
    }

    private static final class ProcessorProvider implements TableFunctionProcessorProvider {
        private final S3CsvService csvService;

        private ProcessorProvider(S3CsvService csvService) {
            this.csvService = csvService;
        }

        @Override
        public TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle) {
            if (!(handle instanceof Handle csvHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
            }
            LOG.info("Creating data processor for path {}", csvHandle.getS3Path());
            return new Processor(csvService, csvHandle);
        }

        @Override
        public TableFunctionSplitProcessor getSplitProcessor(ConnectorSession session, ConnectorTableFunctionHandle functionHandle, ConnectorSplit split) {
            if (!(functionHandle instanceof Handle csvHandle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
            }
            LOG.info("Creating split processor for path {}", csvHandle.getS3Path());
            return new SplitProcessor(new Processor(csvService, csvHandle));
        }
    }

    private static final class Processor implements TableFunctionDataProcessor {
        private final S3CsvService csvService;
        private final Handle handle;
        private final List<Type> columnTypes;
        private BufferedReader reader;
        private boolean finished;

        private Processor(S3CsvService csvService, Handle handle) {
            LOG.debug("Creating processor for path {}", handle.getS3Path());
            this.csvService = csvService;
            this.handle = handle;
            this.columnTypes = handle.resolveColumnTypes();
        }

        @Override
        public TableFunctionProcessorState process(List<Optional<Page>> unused) {
            try {
                if (finished) {
                    LOG.info("Processor already finished for path {}", handle.getS3Path());
                    return TableFunctionProcessorState.Finished.FINISHED;
                }
                ensureReader();
                PageBuilder pageBuilder = new PageBuilder(handle.batchSizeOrDefault(), columnTypes);
                LOG.info("Starting CSV batch read for path {}", handle.getS3Path());
                while (!pageBuilder.isFull()) {
                    String line = reader.readLine();
                    if (line == null) {
                        LOG.info("Reached end of CSV for path {}", handle.getS3Path());
                        closeReader();
                        break;
                    }
                    LOG.debug("Read line: {}", line);
                    if (line.isBlank()) {
                        LOG.debug("Skipping blank line in CSV for path {}", handle.getS3Path());
                        continue;
                    }
                    String[] values = csvService.parseCsvLine(line, handle.getDelimiter());
                    LOG.debug("Appending row with {} values for path {}", values.length, handle.getS3Path());
                    appendRow(pageBuilder, values);
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
            LOG.info("Opening CSV stream for path {}", handle.getS3Path());
            reader = csvService.openReader(handle.getS3Path());
            try {
                reader.readLine();
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
        private final Processor delegate;

        private SplitProcessor(Processor delegate) {
            this.delegate = delegate;
        }

        @Override
        public TableFunctionProcessorState process() {
            return delegate.process(List.of());
        }
    }
}
