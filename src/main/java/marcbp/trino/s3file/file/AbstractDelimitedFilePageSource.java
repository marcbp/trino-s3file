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
        // Probe before opening the long-lived GET, so alignment cannot exhaust the S3 pool.
        boolean skipFragment = !startsAfterDelimiter();
        recordReader = new ByteDelimitedRecordReader(openStream(), charset, delimiter, trimCarriageReturn);
        if (skipFragment) {
            var fragment = readLine();
            if (fragment.isPresent()) {
                addBytesWithinPrimary(fragment.orElseThrow().bytesConsumed());
            }
        }
        afterSourceOpened();
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
