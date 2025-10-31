package marcbp.trino.s3file.file;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.function.table.TableFunctionDataProcessor;
import marcbp.trino.s3file.util.S3ClientBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;

import static java.util.Objects.requireNonNull;

/**
 * Base data processor wiring shared by the S3-backed table functions.
 */
public abstract class AbstractFileProcessor<H extends BaseFileHandle>
        implements TableFunctionDataProcessor {
    private final S3ClientBuilder.SessionClient sessionClient;
    private final H handle;
    private final FileSplit split;
    private final Charset charset;
    private final long primaryLength;

    private BufferedReader reader;
    private boolean finished;
    private boolean sessionClosed;
    private long bytesWithinPrimary;

    protected AbstractFileProcessor(
            ConnectorSession session,
            S3ClientBuilder s3ClientBuilder,
            H handle,
            FileSplit split) {
        this.sessionClient = requireNonNull(s3ClientBuilder, "s3ClientBuilder is null").forSession(session);
        this.handle = requireNonNull(handle, "handle is null");
        this.split = (split != null) ? split : handle.toWholeFileSplit();
        this.charset = handle.charset();
        this.primaryLength = this.split.getPrimaryLength();
    }

    protected S3ClientBuilder.SessionClient sessionClient() {
        return sessionClient;
    }

    protected H handle() {
        return handle;
    }

    protected FileSplit split() {
        return split;
    }

    protected Charset charset() {
        return charset;
    }

    protected long primaryLength() {
        return primaryLength;
    }

    protected long bytesWithinPrimary() {
        return bytesWithinPrimary;
    }

    protected void addBytesWithinPrimary(long delta) {
        bytesWithinPrimary += delta;
    }

    protected void setBytesWithinPrimary(long value) {
        bytesWithinPrimary = value;
    }

    protected BufferedReader reader() {
        return reader;
    }

    protected boolean isFinished() {
        return finished;
    }

    protected void ensureReader() throws IOException {
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

    protected abstract BufferedReader openReader() throws IOException;

    protected void afterReaderOpened(BufferedReader reader) throws IOException {
        // Default no-op.
    }

    protected void handleReaderCloseException(IOException e) {
        throw new UncheckedIOException("Failed to close reader", e);
    }

    protected void closeReader() {
        if (reader == null) {
            return;
        }
        try {
            reader.close();
        }
        catch (IOException e) {
            handleReaderCloseException(e);
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
}
