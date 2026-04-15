package marcbp.trino.s3file.file;

import io.airlift.log.Logger;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public abstract class AbstractTextFilePageSource<H extends BaseTextFileHandle> implements ConnectorPageSource {
    protected final Logger logger;
    protected final S3ClientBuilder.SessionClient sessionClient;
    protected final H handle;
    protected final FileSplit split;
    protected final Charset charset;
    protected final long primaryLength;
    protected final List<S3FileColumnHandle> projectedColumns;

    private final List<Type> projectedTypes;

    protected BufferedReader reader;
    protected boolean finished;
    protected boolean sessionClosed;
    protected long bytesWithinPrimary;
    protected long completedPositions;
    protected long readTimeNanos;
    protected long skippedRows;
    protected long pagesProduced;
    protected long s3Requests;

    protected AbstractTextFilePageSource(
            ConnectorSession session,
            S3ClientBuilder s3ClientBuilder,
            H handle,
            FileSplit split,
            List<S3FileColumnHandle> projectedColumns) {
        this.logger = Logger.get(getClass());
        this.sessionClient = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null").forSession(session);
        this.handle = requireNonNull(handle, "handle is null");
        this.split = requireNonNull(split, "split is null");
        this.charset = handle.charset();
        this.primaryLength = split.getPrimaryLength();
        this.projectedColumns = List.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        List<Type> allTypes = handle.resolveColumnTypes();
        this.projectedTypes = this.projectedColumns.stream()
                .map(column -> allTypes.get(column.getOrdinalPosition()))
                .toList();
    }

    protected final List<Type> projectedTypes() {
        return projectedTypes;
    }

    protected final void addBytesWithinPrimary(long delta) {
        bytesWithinPrimary += delta;
    }

    protected final void recordS3Request() {
        s3Requests++;
    }

    protected final BufferedReader reader() {
        return reader;
    }

    protected boolean finishWhenEmptySplit() {
        return primaryLength == 0;
    }

    protected void afterReaderOpened(BufferedReader reader) throws IOException {
        // Default no-op.
    }

    protected abstract RecordReadResult<?> readNextRecord() throws IOException;

    protected abstract void appendRecord(PageBuilder pageBuilder, Object payload);

    protected final BufferedReader openReader() throws IOException {
        recordS3Request();
        if (split.isWholeFile()) {
            return sessionClient.openReader(
                    handle.getS3Path(),
                    charset,
                    handle.getVersionId(),
                    handle.getETag());
        }
        return sessionClient.openReader(
                handle.getS3Path(),
                split.getStartOffset(),
                split.getRangeEndExclusive(),
                charset,
                handle.getVersionId(),
                handle.getETag());
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

    protected void closeReader() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (IOException e) {
            logger.warn(e, "Failed to close reader for %s", handle.getS3Path());
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
    public final long getCompletedBytes() {
        return Math.min(bytesWithinPrimary, Math.max(0L, primaryLength));
    }

    @Override
    public final OptionalLong getCompletedPositions() {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public final long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public final boolean isFinished() {
        return finished;
    }

    @Override
    public final SourcePage getNextSourcePage() {
        long startNanos = System.nanoTime();
        try {
            if (finished) {
                closeSession();
                return null;
            }
            ensureReader();
            if (finished) {
                closeSession();
                return null;
            }

            PageBuilder pageBuilder = new PageBuilder(handle.getBatchSize(), projectedTypes);
            while (!pageBuilder.isFull()) {
                RecordReadResult<?> result = readNextRecord();
                if (result.status() == RecordStatus.END) {
                    completeProcessing();
                    break;
                }
                if (result.bytesConsumed() > 0) {
                    addBytesWithinPrimary(result.bytesConsumed());
                }
                if (result.status() == RecordStatus.SKIP) {
                    skippedRows++;
                    continue;
                }

                appendRecord(pageBuilder, result.payload());
                completedPositions++;
                if (result.finishesSplit()) {
                    completeProcessing();
                    break;
                }
            }

            if (pageBuilder.isEmpty()) {
                markFinished();
                return null;
            }
            Page page = pageBuilder.build();
            pagesProduced++;
            return SourcePage.create(page);
        }
        catch (IOException e) {
            closeSession();
            throw new UncheckedIOException("Failed to process data for " + handle.getS3Path(), e);
        }
        catch (RuntimeException e) {
            closeSession();
            throw e;
        }
        finally {
            readTimeNanos += System.nanoTime() - startNanos;
        }
    }

    @Override
    public long getMemoryUsage() {
        return 0;
    }

    @Override
    public void close() {
        if (finished) {
            closeSession();
            return;
        }
        finished = true;
        closeReader();
        closeSession();
    }

    public enum RecordStatus {
        END,
        SKIP,
        PRODUCE
    }

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
