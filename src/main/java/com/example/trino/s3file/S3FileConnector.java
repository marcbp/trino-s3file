package com.example.trino.s3file;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import com.example.trino.s3file.S3FileLogger;
import io.trino.spi.connector.ConnectorSplitSource;
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

import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Connector exposing the CSV table function backed by S3.
 */
public final class S3FileConnector implements Connector {
    private static final S3FileLogger LOG = S3FileLogger.get(S3FileConnector.class);

    private final S3CsvService csvService;
    private final S3FileTableFunction tableFunction;
    private final FunctionProvider functionProvider;
    private final ConnectorSplitManager splitManager;

    public S3FileConnector() {
        this(new S3CsvService());
    }

    public S3FileConnector(S3CsvService csvService) {
        this.csvService = requireNonNull(csvService, "csvService is null");
        this.tableFunction = new S3FileTableFunction(csvService);
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
        return Set.of(tableFunction);
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
        csvService.close();
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
            if (!(functionHandle instanceof S3FileTableFunction.Handle handle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
            }
            LOG.info("Supplying processor provider for s3file path {}", handle.getS3Path());
            return tableFunction.createProcessorProvider();
        }
    }

    private final class InlineSplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableFunctionHandle functionHandle) {
            if (!(functionHandle instanceof S3FileTableFunction.Handle handle)) {
                throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
            }
            LOG.info("Providing single split for path {}", handle.getS3Path());
            return new FixedSplitSource(tableFunction.createSplit());
        }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
            throw new UnsupportedOperationException("Not supported");
        }
    }
}
