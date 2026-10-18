package marcbp.trino.s3file.file;

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.VarcharType;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.airlift.slice.SizeOf.sizeOfCharArray;

public abstract class AbstractTextFilePageSource<H extends BaseTextFileHandle> implements ConnectorPageSource {
    protected static final int READER_BUFFER_SIZE = 8192;
    // BufferedReader's character buffer and the JDK InputStreamReader's byte buffer.
    // Opaque parser/client internals and pages handed to Trino are not included.
    private static final long BUFFERED_READER_MEMORY = instanceSize(BufferedReader.class)
            + instanceSize(InputStreamReader.class)
            + sizeOfCharArray(READER_BUFFER_SIZE)
            + sizeOfByteArray(READER_BUFFER_SIZE);
    private final long instanceSize = instanceSize(getClass());

    protected final Logger logger;
    protected final S3ClientBuilder.SessionClient sessionClient;
    protected final H handle;
    protected final FileSplit split;
    protected final Charset charset;
    protected final long primaryLength;
    protected final List<S3FileColumnHandle> projectedColumns;

    private final List<Type> projectedTypes;
    private final List<S3FileColumnHandle> outputColumns;

    protected BufferedReader reader;
    protected boolean finished;
    protected boolean sessionClosed;
    private boolean sourceOpened;
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
        this.outputColumns = List.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.projectedColumns = outputColumns.stream()
                .filter(column -> column.getOrdinalPosition() < handle.dataColumnNames().size())
                .toList();
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

    protected void openSource() throws IOException {
        reader = openReader();
        afterReaderOpened(reader);
    }

    protected abstract RecordReadResult<?> readNextRecord() throws IOException;

    protected abstract void appendRecord(PageBuilder pageBuilder, Object payload);

    protected final BufferedReader openReader() throws IOException {
        recordS3Request();
        if (split.isWholeFile()) {
            return sessionClient.openReader(
                    split.object().path(),
                    charset,
                    split.object().versionIdRef(),
                    split.object().eTagRef());
        }
        return sessionClient.openReader(
                split.object().path(),
                split.getStartOffset(),
                split.getRangeEndExclusive(),
                charset,
                split.object().versionIdRef(),
                split.object().eTagRef());
    }

    protected final InputStream openStream() throws IOException {
        recordS3Request();
        if (split.isWholeFile()) {
            return sessionClient.openStream(
                    split.object().path(),
                    0,
                    null,
                    split.object().versionIdRef(),
                    split.object().eTagRef());
        }
        return sessionClient.openStream(
                split.object().path(),
                split.getStartOffset(),
                split.getRangeEndExclusive(),
                split.object().versionIdRef(),
                split.object().eTagRef());
    }

    private void ensureReader() throws IOException {
        if (sourceOpened || finished) {
            return;
        }
        if (finishWhenEmptySplit()) {
            markFinished();
            return;
        }
        openSource();
        sourceOpened = true;
    }

    protected void closeReader() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (IOException e) {
            logger.warn(e, "Failed to close reader for %s", split.object().path());
        }
        finally {
            reader = null;
            sourceOpened = false;
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

            PageBuilder pageBuilder = new PageBuilder(handle.scan().page().batchSize(), projectedTypes);
            long pageStartBytes = bytesWithinPrimary;
            recordLoop:
            while (!pageBuilder.isFull()) {
                if (!split.isLast() && bytesWithinPrimary >= primaryLength) {
                    completeProcessing();
                    break;
                }
                switch (readNextRecord()) {
                    case RecordReadResult.End<?> ignored -> {
                        completeProcessing();
                        break recordLoop;
                    }
                    case RecordReadResult.Skip<?> skipped -> {
                        if (skipped.bytesConsumed() > 0) {
                            addBytesWithinPrimary(skipped.bytesConsumed());
                        }
                        skippedRows++;
                        continue;
                    }
                    case RecordReadResult.Produce<?> produced -> {
                        if (produced.bytesConsumed() > 0) {
                            addBytesWithinPrimary(produced.bytesConsumed());
                        }
                        appendRecord(pageBuilder, produced.payload());
                        completedPositions++;
                        if (produced.finishesSplit()) {
                            completeProcessing();
                            break recordLoop;
                        }
                    }
                }
                if (bytesWithinPrimary - pageStartBytes >= handle.scan().page().targetSizeBytes()) {
                    break;
                }
            }

            if (pageBuilder.isEmpty()) {
                markFinished();
                return null;
            }
            Page page = pageBuilder.build();
            if (outputColumns.size() != projectedColumns.size()) {
                Block[] blocks = new Block[outputColumns.size()];
                var pathBuilder = VarcharType.VARCHAR.createBlockBuilder(null, 1);
                VarcharType.VARCHAR.writeSlice(pathBuilder, Slices.utf8Slice(split.object().path()));
                var path = RunLengthEncodedBlock.create(pathBuilder.build(), page.getPositionCount());
                int dataIndex = 0;
                for (int i = 0; i < outputColumns.size(); i++) {
                    blocks[i] = outputColumns.get(i).getOrdinalPosition() == handle.dataColumnNames().size()
                            ? path : page.getBlock(dataIndex++);
                }
                page = new Page(page.getPositionCount(), blocks);
            }
            pagesProduced++;
            return SourcePage.create(page);
        }
        catch (IOException e) {
            close();
            throw new UncheckedIOException("Failed to process data for " + split.object().path(), e);
        }
        catch (RuntimeException e) {
            close();
            throw e;
        }
        finally {
            readTimeNanos += System.nanoTime() - startNanos;
        }
    }

    @Override
    public long getMemoryUsage() {
        return instanceSize + (reader == null ? 0 : BUFFERED_READER_MEMORY);
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

    public sealed interface RecordReadResult<T>
            permits RecordReadResult.End, RecordReadResult.Skip, RecordReadResult.Produce {
        static RecordReadResult<?> finished() {
            return new End<>();
        }

        static <T> RecordReadResult<T> skip(long bytesConsumed) {
            return new Skip<>(bytesConsumed);
        }

        static <T> RecordReadResult<T> produce(T payload, long bytesConsumed, boolean finishesSplit) {
            return new Produce<>(requireNonNull(payload, "payload is null"), bytesConsumed, finishesSplit);
        }

        record End<T>() implements RecordReadResult<T> {}

        record Skip<T>(long bytesConsumed) implements RecordReadResult<T> {}

        record Produce<T>(T payload, long bytesConsumed, boolean finishesSplit) implements RecordReadResult<T> {
            public Produce {
                payload = requireNonNull(payload, "payload is null");
            }
        }
    }
}
