package marcbp.trino.s3file.file;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

/**
 * Streams byte-delimited text records while preserving the exact byte count consumed from S3.
 */
public final class ByteDelimitedRecordReader implements Closeable {
    private static final int INSTANCE_SIZE = instanceSize(ByteDelimitedRecordReader.class);

    private final InputStream input;
    private byte[] buffer = new byte[8192];
    private final Charset charset;
    private final byte[] delimiter;
    private final Charset recordCharset;
    private final int characterWidth;
    private int pendingFirst = -1;
    private int pendingSecond = -1;
    private int bomBytes;
    private final boolean trimCarriageReturnBeforeLineFeed;

    public ByteDelimitedRecordReader(
            InputStream input,
            Charset charset,
            byte[] delimiter,
            boolean trimCarriageReturnBeforeLineFeed) throws IOException {
        this.input = requireNonNull(input, "input is null");
        this.charset = requireNonNull(charset, "charset is null");
        requireNonNull(delimiter, "delimiter is null");
        Charset encoding = charset;
        if (StandardCharsets.UTF_16.equals(charset)) {
            int first = input.read();
            int second = input.read();
            encoding = StandardCharsets.UTF_16BE;
            if (first == 0xfe && second == 0xff) {
                bomBytes = 2;
            }
            else if (first == 0xff && second == 0xfe) {
                encoding = StandardCharsets.UTF_16LE;
                bomBytes = 2;
            }
            else {
                pendingFirst = first;
                pendingSecond = second;
            }
        }
        this.recordCharset = encoding;
        this.characterWidth = characterWidth(encoding);
        this.delimiter = StandardCharsets.UTF_16.equals(charset)
                ? new String(delimiter, charset).getBytes(encoding) : delimiter.clone();
        if (this.delimiter.length == 0) {
            throw new IllegalArgumentException("delimiter must not be empty");
        }
        this.trimCarriageReturnBeforeLineFeed = trimCarriageReturnBeforeLineFeed;
    }

    public Optional<Record> readNext() throws IOException {
        int size = 0;
        int next;
        while ((next = readByte()) != -1) {
            if (size == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length * 2);
            }
            buffer[size] = (byte) next;
            size++;
            if (size % characterWidth == 0 && endsWith(buffer, size, delimiter)) {
                int contentLength = size - delimiter.length;
                byte[] content = Arrays.copyOf(buffer, contentLength);
                content = trimCarriageReturnBeforeLineFeed(content);
                return Optional.of(record(content, size, true));
            }
        }

        if (size == 0) {
            return Optional.empty();
        }
        byte[] remaining = Arrays.copyOf(buffer, size);
        return Optional.of(record(remaining, remaining.length, false));
    }

    static int characterWidth(Charset charset) {
        return StandardCharsets.UTF_16LE.equals(charset) || StandardCharsets.UTF_16BE.equals(charset) ? 2 : 1;
    }

    private int readByte() throws IOException {
        if (pendingFirst >= 0) {
            int value = pendingFirst;
            pendingFirst = pendingSecond;
            pendingSecond = -1;
            return value;
        }
        return input.read();
    }

    private Record record(byte[] content, int size, boolean terminated) {
        long consumed = (long) size + bomBytes;
        bomBytes = 0;
        // Keep the caller's encoding contract, including little-endian BOM input.
        if (!recordCharset.equals(charset)) {
            content = new String(content, recordCharset).getBytes(charset);
        }
        return new Record(content, consumed, terminated);
    }

    private byte[] trimCarriageReturnBeforeLineFeed(byte[] content) {
        if (!trimCarriageReturnBeforeLineFeed || !new String(delimiter, recordCharset).endsWith("\n")) {
            return content;
        }
        byte[] carriageReturn = "\r".getBytes(recordCharset);
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

    // Count this reader and its actual arrays, including buffer growth. The input is
    // accounted by the page source; temporary record builders and returned records are excluded.
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE + sizeOf(buffer) + sizeOf(delimiter);
    }

    @Override
    public void close() throws IOException {
        input.close();
    }

    public record Record(byte[] valueBytes, long bytesConsumed, boolean terminated) {
        public Record {
            valueBytes = requireNonNull(valueBytes, "valueBytes is null").clone();
        }

        public String value(Charset charset) {
            return new String(valueBytes, charset);
        }

        public boolean isBlank(Charset charset) {
            if (StandardCharsets.UTF_8.equals(charset)) {
                for (byte valueByte : valueBytes) {
                    if (!isJsonWhitespace(valueByte)) {
                        return false;
                    }
                }
                return true;
            }
            return value(charset).isBlank();
        }

        private static boolean isJsonWhitespace(byte value) {
            return value == ' ' || value == '\t' || value == '\n' || value == '\r';
        }
    }
}
