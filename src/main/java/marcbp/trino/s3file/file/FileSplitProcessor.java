package marcbp.trino.s3file.file;

import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import static java.util.Objects.requireNonNull;

/**
 * Generic {@link TableFunctionSplitProcessor} that delegates to a data processor.
 */
public final class FileSplitProcessor implements TableFunctionSplitProcessor {
    private final TableFunctionDataProcessor dataProcessor;

    public FileSplitProcessor(TableFunctionDataProcessor dataProcessor) {
        this.dataProcessor = requireNonNull(dataProcessor, "dataProcessor is null");
    }

    @Override
    public TableFunctionProcessorState process() {
        return dataProcessor.process(null);
    }
}
