package marcbp.trino.s3file;

import io.airlift.log.Logger;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;
import marcbp.trino.s3file.csv.CsvTableFunction;
import marcbp.trino.s3file.json.JsonTableFunction;
import marcbp.trino.s3file.txt.TextTableFunction;
import marcbp.trino.s3file.xml.XmlTableFunction;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import marcbp.trino.s3file.s3.S3ClientConfig;
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
    private final XmlTableFunction xmlTableFunction;
    private final ConnectorMetadata metadata;
    private final ConnectorSplitManager splitManager;
    private final ConnectorPageSourceProvider pageSourceProvider;

    public S3FileConnector() {
        this(S3ClientConfig.defaults());
    }

    public S3FileConnector(S3ClientConfig clientConfig) {
        this.s3ClientBuilder = new S3ClientBuilder(requireNonNull(clientConfig, "clientConfig is null"));
        this.csvTableFunction = new CsvTableFunction(s3ClientBuilder, clientConfig.splitSizeBytes());
        this.textTableFunction = new TextTableFunction(s3ClientBuilder, clientConfig.splitSizeBytes());
        this.jsonTableFunction = new JsonTableFunction(s3ClientBuilder, clientConfig.splitSizeBytes());
        this.xmlTableFunction = new XmlTableFunction(s3ClientBuilder);
        this.metadata = new S3FileMetadata();
        this.splitManager = new InlineSplitManager();
        this.pageSourceProvider = new S3FilePageSourceProvider(csvTableFunction, textTableFunction, jsonTableFunction, xmlTableFunction);
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
        return TransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle) {
        return metadata;
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions() {
        return Set.of(csvTableFunction, textTableFunction, jsonTableFunction, xmlTableFunction);
    }

    @Override
    public ConnectorSplitManager getSplitManager() {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider() {
        return pageSourceProvider;
    }

    @Override
    public void shutdown() {
        s3ClientBuilder.close();
    }

    private enum TransactionHandle implements ConnectorTransactionHandle {
        INSTANCE
    }

    private final class InlineSplitManager implements ConnectorSplitManager {
        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, io.trino.spi.function.table.ConnectorTableFunctionHandle functionHandle) {
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
            if (functionHandle instanceof XmlTableFunction.Handle xmlHandle) {
                List<FileSplit> splits = xmlTableFunction.createSplits(xmlHandle);
                LOG.info("Providing %s XML split(s) for path %s", splits.size(), xmlHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            throw new IllegalArgumentException("Unexpected handle type: " + functionHandle.getClass().getName());
        }

        @Override
        public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorTableHandle table, DynamicFilter dynamicFilter, Constraint constraint) {
            if (table instanceof CsvTableFunction.Handle csvHandle) {
                List<FileSplit> splits = csvTableFunction.createSplits(csvHandle);
                LOG.info("Providing %s CSV split(s) for table scan path %s", splits.size(), csvHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (table instanceof TextTableFunction.Handle textHandle) {
                List<FileSplit> splits = textTableFunction.createSplits(textHandle);
                LOG.info("Providing %s text split(s) for table scan path %s", splits.size(), textHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (table instanceof JsonTableFunction.Handle jsonHandle) {
                List<FileSplit> splits = jsonTableFunction.createSplits(jsonHandle);
                LOG.info("Providing %s JSON split(s) for table scan path %s", splits.size(), jsonHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            if (table instanceof XmlTableFunction.Handle xmlHandle) {
                List<FileSplit> splits = xmlTableFunction.createSplits(xmlHandle);
                LOG.info("Providing %s XML split(s) for table scan path %s", splits.size(), xmlHandle.getS3Path());
                return new FixedSplitSource(splits);
            }
            throw new IllegalArgumentException("Unexpected table handle type: " + table.getClass().getName());
        }
    }
}
