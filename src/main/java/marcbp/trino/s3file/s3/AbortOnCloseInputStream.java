package marcbp.trino.s3file.s3;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.IOException;
import java.io.InputStream;

import static java.util.Objects.requireNonNull;

/**
 * Closes incomplete S3 responses without letting Apache HTTP download their unused tail.
 */
final class AbortOnCloseInputStream extends InputStream {
    private final ResponseInputStream<GetObjectResponse> response;
    private long remaining;
    private boolean closed;

    AbortOnCloseInputStream(ResponseInputStream<GetObjectResponse> response) {
        this.response = requireNonNull(response, "response is null");
        Long length = response.response().contentLength();
        remaining = length == null ? -1 : length;
    }

    @Override
    public int read() throws IOException {
        int value = response.read();
        consume(value < 0 ? -1 : 1);
        return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        int count = response.read(bytes, offset, length);
        consume(count);
        return count;
    }

    @Override
    public long skip(long count) throws IOException {
        long skipped = response.skip(count);
        consume(skipped);
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return response.available();
    }

    private void consume(long count) {
        if (count < 0) {
            remaining = 0;
        }
        else if (remaining >= 0) {
            remaining = Math.max(0, remaining - count);
        }
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        if (remaining != 0) {
            response.abort();
            return;
        }
        response.close();
    }
}
