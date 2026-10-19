package marcbp.trino.s3file.file;

import io.trino.spi.TrinoException;
import marcbp.trino.s3file.s3.S3ClientBuilder;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/** One streaming S3 read, with separate physical and decoded byte counters. */
public final class FileInput extends InputStream {
    private static final int INSTANCE_SIZE = instanceSize(FileInput.class);
    private final S3ObjectRef object;
    private InputStream decoded;
    private final long streamRetainedSize;
    private IOException transportFailure;
    private long physicalBytes;
    private long decodedBytes;

    public static FileInput open(S3ClientBuilder.SessionClient client, S3ObjectRef object, long start, Long endExclusive) throws IOException {
        if (object.compression() != Compression.NONE && (start != 0 || endExclusive != null)) {
            throw new IllegalArgumentException("Compressed objects must be read as whole files");
        }
        return new FileInput(client.openStream(object.path(), start, endExclusive, object.versionIdRef(), object.eTagRef()), object);
    }

    private FileInput(InputStream raw, S3ObjectRef object) throws IOException {
        this.object = object;
        var counted = new FilterInputStream(raw) {
            @Override
            public int read() throws IOException {
                int value;
                try {
                    value = in.read();
                }
                catch (IOException e) {
                    transportFailure = e;
                    throw e;
                }
                if (value >= 0) {
                    physicalBytes++;
                }
                return value;
            }

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                int count;
                try {
                    count = in.read(bytes, offset, length);
                }
                catch (IOException e) {
                    transportFailure = e;
                    throw e;
                }
                if (count > 0) {
                    physicalBytes += count;
                }
                return count;
            }
        };
        try {
            decoded = object.compression().wrap(counted);
            streamRetainedSize = instanceSize(counted.getClass()) + object.compression().bufferRetainedSizeInBytes();
        }
        catch (IOException | RuntimeException e) {
            try {
                raw.close();
            }
            catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            if (e instanceof IOException io) {
                throw decodingFailure(io);
            }
            throw e;
        }
    }

    @Override
    public int read() throws IOException {
        if (decoded == null) {
            throw new IOException("Stream is closed");
        }
        try {
            int value = decoded.read();
            if (value >= 0) {
                decodedBytes++;
            }
            return value;
        }
        catch (IOException e) {
            throw decodingFailure(e);
        }
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
        if (decoded == null) {
            throw new IOException("Stream is closed");
        }
        try {
            int count = decoded.read(bytes, offset, length);
            if (count > 0) {
                decodedBytes += count;
            }
            return count;
        }
        catch (IOException e) {
            throw decodingFailure(e);
        }
    }

    @Override
    public int available() throws IOException {
        return decoded == null ? 0 : decoded.available();
    }

    private IOException decodingFailure(IOException failure) {
        if (transportFailure != null) {
            return transportFailure;
        }
        if (object.compression() != Compression.NONE) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT,
                    "Invalid " + object.compression() + " content in " + object.path() + ": " + failure.getMessage(), failure);
        }
        return failure;
    }

    public long physicalBytes() {
        return physicalBytes;
    }

    public long decodedBytes() {
        return decodedBytes;
    }

    // Only our shallow stream objects and compression I/O buffers are counted.
    // Decoder heap/native memory and AWS/HTTP internals are opaque and excluded.
    // S3ObjectRef belongs to the shared handle, so it is not charged again here.
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE + (decoded == null ? 0 : streamRetainedSize);
    }

    @Override
    public void close() throws IOException {
        if (decoded != null) {
            try {
                decoded.close();
            }
            finally {
                decoded = null;
            }
        }
    }
}
