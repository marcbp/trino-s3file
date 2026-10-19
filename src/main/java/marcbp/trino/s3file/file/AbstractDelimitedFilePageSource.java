package marcbp.trino.s3file.file;

import io.trino.spi.connector.ConnectorSession;
import marcbp.trino.s3file.S3FileColumnHandle;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.airlift.slice.SizeOf.sizeOf;

/** Shared byte framing, split alignment and reader ownership for TXT, JSON and CSV. */
public abstract class AbstractDelimitedFilePageSource<H extends BaseTextFileHandle, T>
        extends AbstractTextFilePageSource<H, T> {
    private final byte[] delimiter;
    private final boolean trimCarriageReturn;
    private ByteDelimitedRecordReader recordReader;

    protected AbstractDelimitedFilePageSource(ConnectorSession session, S3ClientBuilder builder,
            H handle, FileSplit split, List<S3FileColumnHandle> columns, String delimiter, boolean trimCarriageReturn) {
        super(session, builder, handle, split, columns);
        this.delimiter = delimiter.getBytes(charset);
        this.trimCarriageReturn = trimCarriageReturn;
    }

    @Override
    protected final boolean finishWhenEmptySplit() {
        return primaryLength == 0 && split.startOffset() > 0;
    }

    @Override
    protected final void openSource() throws IOException {
        if (!split.first() && delimiter.length > 1) {
            long start = alignmentStart();
            recordReader = new ByteDelimitedRecordReader(openStream(start), charset, delimiter, trimCarriageReturn);
            addBytesWithinPrimary(start - split.startOffset());
            while (bytesWithinPrimary < 0) {
                var fragment = readLine();
                if (fragment.isEmpty()) {
                    break;
                }
                addBytesWithinPrimary(fragment.orElseThrow().bytesConsumed());
            }
        }
        else {
            // Probe before opening the long-lived GET, so alignment cannot exhaust the S3 pool.
            boolean skipFragment = !startsAfterDelimiter();
            recordReader = new ByteDelimitedRecordReader(openStream(), charset, delimiter, trimCarriageReturn);
            if (skipFragment) {
                var fragment = readLine();
                if (fragment.isPresent()) {
                    addBytesWithinPrimary(fragment.orElseThrow().bytesConsumed());
                }
            }
        }
        afterSourceOpened();
    }

    private long alignmentStart() throws IOException {
        int width = ByteDelimitedRecordReader.characterWidth(charset);
        long start = Math.max(0, split.startOffset() - delimiter.length);
        start -= start % width;
        boolean overlapping = false;
        for (int shift = width; shift < delimiter.length; shift += width) {
            overlapping |= Arrays.equals(delimiter, shift, delimiter.length, delimiter, 0, delimiter.length - shift);
        }
        if (!overlapping) {
            return start;
        }
        // A self-overlapping delimiter needs a known framing origin. Search backwards
        // in bounded blocks for a code unit that cannot be part of the delimiter.
        // This avoids rescanning the file from zero for ordinary custom separators.
        long end = start;
        while (end > 0) {
            long begin = Math.max(0, end - 8192);
            byte[] preceding = sessionClient.readBytes(split.object().path(), begin, end,
                    split.object().versionIdRef(), split.object().eTagRef());
            for (int i = preceding.length - width; i >= 0; i -= width) {
                boolean inDelimiter = false;
                for (int j = 0; j < delimiter.length; j += width) {
                    inDelimiter |= Arrays.equals(preceding, i, i + width, delimiter, j, j + width);
                }
                if (!inDelimiter) {
                    return begin + i;
                }
            }
            end = begin;
        }
        return 0;
    }

    private boolean startsAfterDelimiter() throws IOException {
        if (split.first()) {
            return true;
        }
        if (delimiter.length == 0 || split.startOffset() < delimiter.length) {
            return false;
        }
        byte[] preceding = sessionClient.readBytes(split.object().path(), split.startOffset() - delimiter.length,
                split.startOffset(), split.object().versionIdRef(), split.object().eTagRef());
        return Arrays.equals(preceding, delimiter);
    }

    protected void afterSourceOpened() throws IOException {}

    protected final Optional<ByteDelimitedRecordReader.Record> readLine() throws IOException {
        return recordReader.readNext();
    }

    @Override
    protected final void closeReader() throws IOException {
        if (recordReader != null) {
            try {
                recordReader.close();
            }
            finally {
                recordReader = null;
            }
        }
    }

    @Override
    public long getMemoryUsage() {
        return super.getMemoryUsage() + sizeOf(delimiter)
                + (recordReader == null ? 0 : recordReader.getRetainedSizeInBytes());
    }
}
