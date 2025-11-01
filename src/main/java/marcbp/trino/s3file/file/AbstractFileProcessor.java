package marcbp.trino.s3file.file;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import io.trino.spi.function.table.TableFunctionProcessorState;
import marcbp.trino.s3file.util.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Base data processor wiring shared by the S3-backed table functions.
 *
 * <p>The template orchestrates processing in three stages:
 * <ol>
 *     <li>Reader management – ensuring a reader is open for the current split.</li>
 *     <li>Row production – repeatedly delegating to the subclass to fetch and append records.</li>
 *     <li>Finalisation – handling empty batches, signalling split completion, and transforming exceptions.</li>
 * </ol>
 */
public abstract class AbstractFileProcessor<H extends BaseFileHandle>
        implements TableFunctionDataProcessor {
    protected final Logger logger;
    protected final S3ClientBuilder.SessionClient sessionClient;
    protected final H handle;
    protected final FileSplit split;
    protected final Charset charset;
    protected final long primaryLength;

    protected BufferedReader reader;
    protected boolean finished;
    protected boolean sessionClosed;
    protected long bytesWithinPrimary;

    protected AbstractFileProcessor(
            ConnectorSession session,
            S3ClientBuilder s3ClientBuilder,
            H handle,
            FileSplit split) {
        this.logger = Logger.get(getClass());
        this.sessionClient = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null").forSession(session);
        this.handle = requireNonNull(handle, "handle is null");
        this.split = (split != null) ? split : handle.toWholeFileSplit();
        this.charset = handle.charset();
        this.primaryLength = this.split.getPrimaryLength();
    }

    protected void addBytesWithinPrimary(long delta) {
        bytesWithinPrimary += delta;
    }

    protected BufferedReader reader() {
        return reader;
    }

    private boolean isFinished() {
        return finished;
    }

    private void ensureReader() throws IOException {
        if (reader != null || finished) {
            return;
        }
        if (finishWhenEmptySplit()) {
            markFinished();
            return;
        }
        reader = openReader();
        afterReaderOpened(reader);
    }

    protected boolean finishWhenEmptySplit() {
        return primaryLength == 0;
    }

    protected final BufferedReader openReader() throws IOException {
        if (split.isWholeFile()) {
            return sessionClient.openReader(handle.getS3Path(), charset);
        }
        return sessionClient.openReader(handle.getS3Path(), split.getStartOffset(), split.getRangeEndExclusive(), charset);
    }

    protected void afterReaderOpened(BufferedReader reader) throws IOException {
        // Default no-op.
    }

    /**
     * Column types produced by the processor.
     */
    protected abstract List<io.trino.spi.type.Type> columnTypes();

    /**
     * Batch size hint for the page builder.
     */
    protected abstract int batchSize();

    /**
     * Fetches the next logical record from the underlying representation.
     * The returned {@link RecordReadResult} indicates whether data should be appended,
     * skipped, or whether the split has been exhausted.
     */
    protected abstract RecordReadResult<?> readNextRecord() throws IOException;

    /**
     * Appends the supplied payload to the provided {@link PageBuilder}.
     * The payload is the object returned via {@link RecordReadResult#payload()}.
     */
    protected abstract void appendRecord(PageBuilder pageBuilder, Object payload);

    protected void closeReader() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (IOException e) {
            logger.warn(e, "Failed to close reader for %s", handle.getS3Path());
            throw new UncheckedIOException("Failed to close reader for " + handle.getS3Path(), e);
        }
        finally {
            reader = null;
        }
    }

    protected void closeSession() {
        if (sessionClosed) {
            return;
        }
        sessionClosed = true;
        sessionClient.close();
    }

    protected void markFinished() {
        finished = true;
        closeSession();
    }

    protected void completeProcessing() {
        closeReader();
        markFinished();
    }

    @Override
    public final TableFunctionProcessorState process(List<Optional<Page>> unused) {
        try {
            if (isFinished()) {
                logger.debug("Split %s already finished for %s", split.getId(), handle.getS3Path());
                closeSession();
                return TableFunctionProcessorState.Finished.FINISHED;
            }
            ensureReader();
            if (isFinished()) {
                logger.debug("Nothing to process after reader initialisation for %s", handle.getS3Path());
                closeSession();
                return TableFunctionProcessorState.Finished.FINISHED;
            }

            logger.debug("Processing split %s for %s", split.getId(), handle.getS3Path());
            PageBuilder pageBuilder = new PageBuilder(batchSize(), columnTypes());
            while (!pageBuilder.isFull()) {
                RecordReadResult<?> result = readNextRecord();
                if (result.status() == RecordStatus.END) {
                    logger.debug("Reached end of split %s for %s", split.getId(), handle.getS3Path());
                    completeProcessing();
                    break;
                }
                if (result.bytesConsumed() > 0) {
                    addBytesWithinPrimary(result.bytesConsumed());
                }
                if (result.status() == RecordStatus.SKIP) {
                    logger.debug("Skipping record for %s (split %s)", handle.getS3Path(), split.getId());
                    continue;
                }

                appendRecord(pageBuilder, result.payload());
                if (result.finishesSplit()) {
                    logger.debug("Split %s fully processed for %s", split.getId(), handle.getS3Path());
                    completeProcessing();
                    break;
                }
            }

            if (pageBuilder.isEmpty()) {
                logger.debug("No rows produced for split %s (%s)", split.getId(), handle.getS3Path());
                markFinished();
                return TableFunctionProcessorState.Finished.FINISHED;
            }
            Page page = pageBuilder.build();
            logger.debug("Produced %s rows for split %s (%s)", page.getPositionCount(), split.getId(), handle.getS3Path());
            return TableFunctionProcessorState.Processed.produced(page);
        }
        catch (IOException e) {
            logger.error(e, "I/O error while processing %s", handle.getS3Path());
            closeSession();
            throw new UncheckedIOException("Failed to process data for " + handle.getS3Path(), e);
        }
        catch (RuntimeException e) {
            logger.error(e, "Runtime error while processing %s", handle.getS3Path());
            closeSession();
            throw e;
        }
    }

    public enum RecordStatus {
        END,
        SKIP,
        PRODUCE
    }

    /**
     * Encapsulates the outcome of reading a logical record from the input stream.
     */
    public static final class RecordReadResult<T> {
        private final RecordStatus status;
        private final T payload;
        private final long bytesConsumed;
        private final boolean finishesSplit;

        private RecordReadResult(RecordStatus status, T payload, long bytesConsumed, boolean finishesSplit) {
            this.status = status;
            this.payload = payload;
            this.bytesConsumed = bytesConsumed;
            this.finishesSplit = finishesSplit;
        }

        public static RecordReadResult<?> finished() {
            return new RecordReadResult<>(RecordStatus.END, null, 0, false);
        }

        public static <T> RecordReadResult<T> skip(long bytesConsumed) {
            return new RecordReadResult<>(RecordStatus.SKIP, null, bytesConsumed, false);
        }

        public static <T> RecordReadResult<T> produce(T payload, long bytesConsumed, boolean finishesSplit) {
            Objects.requireNonNull(payload, "payload is null");
            return new RecordReadResult<>(RecordStatus.PRODUCE, payload, bytesConsumed, finishesSplit);
        }

        public RecordStatus status() {
            return status;
        }

        public T payload() {
            return payload;
        }

        public long bytesConsumed() {
            return bytesConsumed;
        }

        public boolean finishesSplit() {
            return finishesSplit;
        }
    }
}
