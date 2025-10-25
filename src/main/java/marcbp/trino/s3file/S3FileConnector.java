package marcbp.trino.s3file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.transaction.IsolationLevel;
import marcbp.trino.s3file.csv.CsvProcessingService;
import marcbp.trino.s3file.csv.S3FileCsvTableFunction;
import marcbp.trino.s3file.json.S3FileJsonTableFunction;
import marcbp.trino.s3file.txt.S3FileTextTableFunction;

import java.util.Optional;
import java.util.Set;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Connector exposing table functions backed by S3.
 */
public final class S3FileConnector implements Connector {
    private static final Logger LOG = LoggerFactory.getLogger(S3FileConnector.class);

    private final S3ObjectService s3ObjectService;
    private final CsvProcessingService csvProcessingService;
    private final S3FileCsvTableFunction csvTableFunction;
    private final S3FileTextTableFunction textTableFunction;
    private final S3FileJsonTableFunction jsonTableFunction;
    private final FunctionProvider functionProvider;
    private final ConnectorSplitManager splitManager;

    public S3FileConnector() {
        this(new S3ObjectService(), new CsvProcessingService());
    }

    public S3FileConnector(S3ObjectService s3ObjectService, CsvProcessingService csvProcessingService) {
        this.s3ObjectService = requireNonNull(s3ObjectService, "s3ObjectService is null");
        this.csvProcessingService = requireNonNull(csvProcessingService, "csvProcessingService is null");
        this.csvTableFunction = new S3FileCsvTableFunction(s3ObjectService, csvProcessingService);
        this.textTableFunction = new S3FileTextTableFunction(s3ObjectService);
        this.jsonTableFunction = new S3FileJsonTableFunction(s3ObjectService);
        this.functionProvider = new InlineFunctionProvider();
        this.splitManager = new InlineSplitManager();
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return TransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return InlineMetadata.INSTANCE;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return Set.of(csvTableFunction, textTableFunction, jsonTableFunction);
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider() {
        return Optional.of(functionProvider);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public void shutdown() {
        s3ObjectService.close();
    }

    private enum TransactionHandle implements ConnectorTransactionHandle {
        INSTANCE
    }

    private static final class InlineMetadata implements ConnectorMetadata {
        private static final InlineMetadata INSTANCE = new InlineMetadata();
    }

    private final class InlineFunctionProvider implements FunctionProvider {
        @Override
        public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle) {
            if (functionHandle instanceof S3FileCsvTableFunction.Handle csvHandle) {
                LOG.info("Supplying CSV processor provider for path {}", csvHandle.getS3Path());
                return csvTableFunction.createProcessorProvider();
            }
            if (functionHandle instanceof S3FileTextTableFunction.Handle textHandle) {
                LOG.info("Supplying text processor provider for path {}", textHandle.getS3Path());
                return textTableFunction.createProcessorProvider();
            }
            if (functionHandle instanceof S3FileJsonTableFunction.Handle jsonHandle) {
                LOG.info("Supplying JSON processor provider for path {}", jsonHandle.getS3Path());
                return jsonTableFunction.createProcessorProvider();
            }
            throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
        }
    }

    private final class InlineSplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableFunctionHandle functionHandle) {
            if (functionHandle instanceof S3FileCsvTableFunction.Handle csvHandle) {
                List<ConnectorSplit> splits = csvTableFunction.createSplits(csvHandle);
                LOG.info("Providing {} CSV split(s) for path {}", splits.size(), csvHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (functionHandle instanceof S3FileTextTableFunction.Handle textHandle) {
                List<ConnectorSplit> splits = textTableFunction.createSplits(textHandle);
                LOG.info("Providing {} text split(s) for path {}", splits.size(), textHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (functionHandle instanceof S3FileJsonTableFunction.Handle jsonHandle) {
                List<ConnectorSplit> splits = jsonTableFunction.createSplits(jsonHandle);
                LOG.info("Providing {} JSON split(s) for path {}", splits.size(), jsonHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
        }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
            throw new UnsupportedOperationException("Not supported");
        }
    }
}
