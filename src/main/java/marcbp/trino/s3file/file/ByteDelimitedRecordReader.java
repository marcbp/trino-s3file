package marcbp.trino.s3file.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Streams byte-delimited text records while preserving the exact byte count consumed from S3.
 */
public final class ByteDelimitedRecordReader implements Closeable {
    private final InputStream input;
    private final Charset charset;
    private final byte[] delimiter;
    private final boolean trimCarriageReturnBeforeLineFeed;

    public ByteDelimitedRecordReader(
            InputStream input,
            Charset charset,
            byte[] delimiter,
            boolean trimCarriageReturnBeforeLineFeed) {
        this.input = requireNonNull(input, "input is null");
        this.charset = requireNonNull(charset, "charset is null");
        this.delimiter = requireNonNull(delimiter, "delimiter is null").clone();
        if (this.delimiter.length == 0) {
            throw new IllegalArgumentException("delimiter must not be empty");
        }
        this.trimCarriageReturnBeforeLineFeed = trimCarriageReturnBeforeLineFeed;
    }

    public Optional<Record> readNext() throws IOException {
        byte[] bytes = new byte[8192];
        int size = 0;
        int next;
        while ((next = input.read()) != -1) {
            if (size == bytes.length) {
                bytes = Arrays.copyOf(bytes, bytes.length * 2);
            }
            bytes[size] = (byte) next;
            size++;
            if (endsWith(bytes, size, delimiter)) {
                int contentLength = size - delimiter.length;
                byte[] content = Arrays.copyOf(bytes, contentLength);
                content = trimCarriageReturnBeforeLineFeed(content);
                return Optional.of(new Record(new String(content, charset), size, true));
            }
        }

        if (size == 0) {
            return Optional.empty();
        }
        byte[] remaining = Arrays.copyOf(bytes, size);
        return Optional.of(new Record(new String(remaining, charset), remaining.length, false));
    }

    private byte[] trimCarriageReturnBeforeLineFeed(byte[] content) {
        if (!trimCarriageReturnBeforeLineFeed || delimiter.length == 0 || delimiter[delimiter.length - 1] != '\n') {
            return content;
        }
        byte[] carriageReturn = "\r".getBytes(charset);
        if (!endsWith(content, content.length, carriageReturn)) {
            return content;
        }
        return Arrays.copyOf(content, content.length - carriageReturn.length);
    }

    private static boolean endsWith(byte[] value, int length, byte[] suffix) {
        if (length < suffix.length) {
            return false;
        }
        int offset = length - suffix.length;
        for (int index = 0; index < suffix.length; index++) {
            if (value[offset + index] != suffix[index]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    public record Record(String value, long bytesConsumed, boolean terminated) {}
}
