package marcbp.trino.s3file;

import io.airlift.log.Logger;
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
import marcbp.trino.s3file.csv.CsvTableFunction;
import marcbp.trino.s3file.json.JsonTableFunction;
import marcbp.trino.s3file.txt.TextTableFunction;
import marcbp.trino.s3file.util.S3ClientBuilder;
import marcbp.trino.s3file.util.S3ClientConfig;
import marcbp.trino.s3file.file.FileSplit;

import java.util.Optional;
import java.util.Set;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Connector exposing table functions backed by S3.
 */
public final class S3FileConnector implements Connector {
    private static final Logger LOG = Logger.get(S3FileConnector.class);

    private final S3ClientBuilder s3ClientBuilder;
    private final CsvTableFunction csvTableFunction;
    private final TextTableFunction textTableFunction;
    private final JsonTableFunction jsonTableFunction;
    private final FunctionProvider functionProvider;
    private final ConnectorSplitManager splitManager;

    public S3FileConnector() {
        this(S3ClientConfig.defaults());
    }

    public S3FileConnector(S3ClientConfig clientConfig) {
        this.s3ClientBuilder = new S3ClientBuilder(requireNonNull(clientConfig, "clientConfig is null"));
        CsvProcessingService csvProcessingService = new CsvProcessingService();
        this.csvTableFunction = new CsvTableFunction(s3ClientBuilder, csvProcessingService);
        this.textTableFunction = new TextTableFunction(s3ClientBuilder);
        this.jsonTableFunction = new JsonTableFunction(s3ClientBuilder);
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
            if (functionHandle instanceof CsvTableFunction.Handle csvHandle) {
                LOG.info("Supplying CSV processor provider for path %s", csvHandle.getS3Path());
                return csvTableFunction.createProcessorProvider();
            }
            if (functionHandle instanceof TextTableFunction.Handle textHandle) {
                LOG.info("Supplying text processor provider for path %s", textHandle.getS3Path());
                return textTableFunction.createProcessorProvider();
            }
            if (functionHandle instanceof JsonTableFunction.Handle jsonHandle) {
                LOG.info("Supplying JSON processor provider for path %s", jsonHandle.getS3Path());
                return jsonTableFunction.createProcessorProvider();
            }
            throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
        }
    }

    private final class InlineSplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableFunctionHandle functionHandle) {
            if (functionHandle instanceof CsvTableFunction.Handle csvHandle) {
                List<FileSplit> splits = csvTableFunction.createSplits(csvHandle);
                LOG.info("Providing %s CSV split(s) for path %s", splits.size(), csvHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (functionHandle instanceof TextTableFunction.Handle textHandle) {
                List<FileSplit> splits = textTableFunction.createSplits(textHandle);
                LOG.info("Providing %s text split(s) for path %s", splits.size(), textHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (functionHandle instanceof JsonTableFunction.Handle jsonHandle) {
                List<FileSplit> splits = jsonTableFunction.createSplits(jsonHandle);
                LOG.info("Providing %s JSON split(s) for path %s", splits.size(), jsonHandle.getS3Path());
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
