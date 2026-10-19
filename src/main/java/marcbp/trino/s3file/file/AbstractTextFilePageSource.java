package marcbp.trino.s3file.file;

import io.airlift.log.Logger;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.OptionalLong;

import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public abstract class AbstractTextFilePageSource<H extends BaseTextFileHandle, T> implements ConnectorPageSource {
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

    private FileInput input;
    private long physicalBytes;
    private long alignmentBytes;
    private boolean finished;
    protected long bytesWithinPrimary;
    protected long completedPositions;
    protected long readTimeNanos;

    protected AbstractTextFilePageSource(
            ConnectorSession session,
            S3ClientBuilder s3ClientBuilder,
            H handle,
            FileSplit split,
            List<S3FileColumnHandle> projectedColumns) {
        this.logger = Logger.get(getClass());
        this.handle = requireNonNull(handle, "handle is null");
        this.split = requireNonNull(split, "split is null");
        this.charset = handle.charset();
        this.primaryLength = split.primaryLength();
        this.outputColumns = List.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        this.projectedColumns = outputColumns.stream()
                .filter(column -> column.getOrdinalPosition() < handle.dataColumnNames().size())
                .toList();
        List<Type> allTypes = handle.resolveColumnTypes();
        this.projectedTypes = this.projectedColumns.stream()
                .map(column -> allTypes.get(column.getOrdinalPosition()))
                .toList();
        this.sessionClient = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null").forSession(session);
    }

    protected final List<Type> projectedTypes() {
        return projectedTypes;
    }

    protected final void addBytesWithinPrimary(long delta) {
        bytesWithinPrimary += delta;
    }

    protected boolean finishWhenEmptySplit() {
        return primaryLength == 0 && split.object().compression() == Compression.NONE;
    }

    protected abstract void openSource() throws IOException;

    protected abstract RecordReadResult<T> readNextRecord() throws IOException;

    protected abstract void appendRecord(PageBuilder pageBuilder, T payload);

    protected abstract void closeReader() throws IOException;

    protected final InputStream openStream() throws IOException {
        return openStream(split.startOffset());
    }

    protected final InputStream openStream(long start) throws IOException {
        alignmentBytes = split.startOffset() - start;
        input = FileInput.open(sessionClient, split.object(), start,
                split.isWholeFile() ? null : split.rangeEndExclusive());
        return input;
    }

    @Override
    public final long getCompletedBytes() {
        return Math.min(Math.max(0L, (input == null ? physicalBytes : input.physicalBytes()) - alignmentBytes), Math.max(0L, primaryLength));
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
                return null;
            }
            if (finishWhenEmptySplit()) {
                close();
                return null;
            }
            if (input == null) {
                openSource();
            }

            PageBuilder pageBuilder = new PageBuilder(handle.scan().page().batchSize(), projectedTypes);
            long pageStartBytes = input.decodedBytes();
            recordLoop:
            while (!pageBuilder.isFull()) {
                if (!split.last() && bytesWithinPrimary >= primaryLength) {
                    close();
                    break;
                }
                switch (readNextRecord()) {
                    case RecordReadResult.End<T> ignored -> {
                        close();
                        break recordLoop;
                    }
                    case RecordReadResult.Skip<T> skipped -> {
                        addBytesWithinPrimary(skipped.bytesConsumed());
                        continue;
                    }
                    case RecordReadResult.Produce<T> produced -> {
                        addBytesWithinPrimary(produced.bytesConsumed());
                        appendRecord(pageBuilder, produced.payload());
                        completedPositions++;
                        if (!split.last() && bytesWithinPrimary > primaryLength) {
                            close();
                            break recordLoop;
                        }
                    }
                }
                if (input.decodedBytes() - pageStartBytes >= handle.scan().page().targetSizeBytes()) {
                    break;
                }
            }

            if (pageBuilder.isEmpty()) {
                close();
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

    // Partial retained-heap accounting: reader/stream wrappers and known buffers only.
    // Shared handle metadata, opaque codec/parser/AWS state (including native memory)
    // and transient records/page builders are excluded; emitted pages belong to Trino.
    @Override
    public long getMemoryUsage() {
        return instanceSize + (input == null ? 0 : input.getRetainedSizeInBytes());
    }

    @Override
    public final void close() {
        if (finished) {
            return;
        }
        finished = true;
        try {
            closeReader();
        }
        catch (IOException | RuntimeException e) {
            logger.warn(e, "Failed to close reader for %s", split.object().path());
        }
        finally {
            try {
                if (input != null) {
                    physicalBytes = input.physicalBytes();
                    try {
                        input.close();
                    }
                    finally {
                        input = null;
                    }
                }
            }
            catch (IOException e) {
                logger.warn(e, "Failed to close input for %s", split.object().path());
            }
            finally {
                sessionClient.close();
            }
        }
    }

    public sealed interface RecordReadResult<T>
            permits RecordReadResult.End, RecordReadResult.Skip, RecordReadResult.Produce {
        static <T> RecordReadResult<T> finished() {
            return new End<>();
        }

        static <T> RecordReadResult<T> skip(long bytesConsumed) {
            return new Skip<>(bytesConsumed);
        }

        static <T> RecordReadResult<T> produce(T payload, long bytesConsumed) {
            return new Produce<>(requireNonNull(payload, "payload is null"), bytesConsumed);
        }

        record End<T>() implements RecordReadResult<T> {}

        record Skip<T>(long bytesConsumed) implements RecordReadResult<T> {}

        record Produce<T>(T payload, long bytesConsumed) implements RecordReadResult<T> {
            public Produce {
                payload = requireNonNull(payload, "payload is null");
            }
        }
    }
}
