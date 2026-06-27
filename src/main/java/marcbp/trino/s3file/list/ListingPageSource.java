package marcbp.trino.s3file.list;

import io.airlift.slice.Slices;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Queue;

import static java.util.Objects.requireNonNull;

public final class ListingPageSource implements ConnectorPageSource {
    private static final int PAGE_BUILDER_BATCH_SIZE = 1024;

    private final S3ClientBuilder.SessionClient sessionClient;
    private final ListingFetcher fetcher;
    private final List<S3FileColumnHandle> projectedColumns;
    private final List<Type> projectedTypes;
    private final Queue<ListingRow> buffer = new ArrayDeque<>();
    private String continuationToken;
    private boolean exhausted;
    private boolean finished;
    private boolean closed;
    private boolean sessionClosed;
    private long completedPositions;
    private long readTimeNanos;

    public ListingPageSource(
            ConnectorSession session,
            S3ClientBuilder s3ClientBuilder,
            ListingFetcher fetcher,
            List<S3FileColumnHandle> projectedColumns,
            List<Type> allTypes) {
        this.sessionClient = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null").forSession(session);
        this.fetcher = requireNonNull(fetcher, "fetcher is null");
        this.projectedColumns = List.copyOf(requireNonNull(projectedColumns, "projectedColumns is null"));
        List<Type> availableTypes = requireNonNull(allTypes, "allTypes is null");
        this.projectedTypes = this.projectedColumns.stream()
                .map(column -> availableTypes.get(column.getOrdinalPosition()))
                .toList();
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public OptionalLong getCompletedPositions() {
        return OptionalLong.of(completedPositions);
    }

    @Override
    public long getReadTimeNanos() {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished() {
        return finished;
    }

    @Override
    public io.trino.spi.connector.SourcePage getNextSourcePage() {
        long startNanos = System.nanoTime();
        try {
            if (finished) {
                closeSession();
                return null;
            }

            PageBuilder pageBuilder = new PageBuilder(PAGE_BUILDER_BATCH_SIZE, projectedTypes);
            while (!pageBuilder.isFull()) {
                if (buffer.isEmpty()) {
                    if (!fetchNextBatch()) {
                        break;
                    }
                    if (buffer.isEmpty()) {
                        continue;
                    }
                }
                ListingRow row = buffer.poll();
                if (row == null) {
                    continue;
                }
                appendRow(pageBuilder, row);
                completedPositions++;
            }

            if (pageBuilder.isEmpty()) {
                markFinished();
                return null;
            }

            return io.trino.spi.connector.SourcePage.create(pageBuilder.build());
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
        if (closed) {
            return;
        }
        closed = true;
        finished = true;
        closeSession();
    }

    private boolean fetchNextBatch() {
        if (finished || exhausted) {
            return false;
        }

        ListingPage page = fetcher.fetch(sessionClient, Optional.ofNullable(continuationToken));
        buffer.addAll(page.rows());
        continuationToken = page.nextContinuationToken().orElse(null);
        if (continuationToken == null) {
            exhausted = true;
        }
        if (buffer.isEmpty() && exhausted) {
            finished = true;
            closeSession();
            return false;
        }
        return true;
    }

    private void appendRow(PageBuilder pageBuilder, ListingRow row) {
        for (int outputIndex = 0; outputIndex < projectedColumns.size(); outputIndex++) {
            S3FileColumnHandle columnHandle = projectedColumns.get(outputIndex);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputIndex);
            Object value = row.valueFor(columnHandle.getName());
            if (value == null) {
                blockBuilder.appendNull();
                continue;
            }
            Type type = projectedTypes.get(outputIndex);
            if (type == BigintType.BIGINT) {
                BigintType.BIGINT.writeLong(blockBuilder, (Long) value);
            }
            else if (type == TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS) {
                TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, (Long) value);
            }
            else {
                VarcharType.createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(value.toString()));
            }
        }
        pageBuilder.declarePosition();
    }

    private void markFinished() {
        finished = true;
        closeSession();
    }

    private void closeSession() {
        if (sessionClosed) {
            return;
        }
        sessionClosed = true;
        sessionClient.close();
    }
}
