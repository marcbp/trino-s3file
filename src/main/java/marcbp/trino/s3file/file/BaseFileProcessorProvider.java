package marcbp.trino.s3file.file;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.util.Objects;

/**
 * Base {@link TableFunctionProcessorProvider} that performs common type checks
 * for table functions operating on {@link FileSplit}s.
 */
public abstract class BaseFileProcessorProvider<H extends BaseFileHandle> implements TableFunctionProcessorProvider {
    private final Class<H> handleType;

    protected BaseFileProcessorProvider(Class<H> handleType) {
        this.handleType = Objects.requireNonNull(handleType, "handleType is null");
    }

    @Override
    public final TableFunctionDataProcessor getDataProcessor(ConnectorSession session, ConnectorTableFunctionHandle handle) {
        if (!handleType.isInstance(handle)) {
            throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
        }
        return createDataProcessor(session, handleType.cast(handle));
    }

    @Override
    public final TableFunctionSplitProcessor getSplitProcessor(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle,
            io.trino.spi.connector.ConnectorSplit split) {
        if (!handleType.isInstance(handle)) {
            throw new IllegalArgumentException("Unexpected handle type: " + handle.getClass().getName());
        }
        if (!(split instanceof FileSplit fileSplit)) {
            throw new IllegalArgumentException("Unexpected split type: " + split.getClass().getName());
        }
        return createSplitProcessor(session, handleType.cast(handle), fileSplit);
    }

    protected abstract TableFunctionDataProcessor createDataProcessor(ConnectorSession session, H handle);

    protected abstract TableFunctionSplitProcessor createSplitProcessor(ConnectorSession session, H handle, FileSplit split);
}
