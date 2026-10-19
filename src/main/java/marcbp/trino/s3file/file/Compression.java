package marcbp.trino.s3file.file;

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.type.VarcharType;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Locale;

import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOfByteArray;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

/** Stream codecs; AUTO is resolved to a concrete codec before splits reach workers. */
public enum Compression {
    NONE(null),
    GZIP(CompressorStreamFactory.GZIP, ".gz", ".gzip"),
    BZIP2(CompressorStreamFactory.BZIP2, ".bz2", ".bzip2"),
    ZSTD(CompressorStreamFactory.ZSTANDARD, ".zst", ".zstd"),
    LZ4_FRAMED(CompressorStreamFactory.LZ4_FRAMED, ".lz4"),
    SNAPPY_FRAMED(CompressorStreamFactory.SNAPPY_FRAMED, ".sz", ".snappy");

    private static final int BUFFER_SIZE = 8192;
    // Shallow sizes and fixed byte arrays of the two BufferedInputStreams we allocate.
    // Codec-owned Java buffers, dictionaries, windows and native allocations are not counted.
    private static final long BUFFER_RETAINED_SIZE = 2L * (instanceSize(BufferedInputStream.class)
            + sizeOfByteArray(BUFFER_SIZE));
    private static final CompressorStreamFactory FACTORY = new CompressorStreamFactory(true);

    private final String format;
    private final String[] extensions;

    Compression(String format, String... extensions) {
        this.format = format;
        this.extensions = extensions;
    }

    public static ScalarArgumentSpecification argument() {
        return ScalarArgumentSpecification.builder()
                .name("COMPRESSION")
                .type(VarcharType.VARCHAR)
                .defaultValue(Slices.utf8Slice("auto"))
                .build();
    }

    public static Compression resolve(String mode, String path) {
        String normalized = mode.trim().toLowerCase(Locale.ROOT);
        if (normalized.equals("auto")) {
            String lowerPath = path.toLowerCase(Locale.ROOT);
            for (Compression codec : values()) {
                for (String extension : codec.extensions) {
                    if (lowerPath.endsWith(extension)) {
                        return codec;
                    }
                }
            }
            return NONE;
        }
        for (Compression codec : values()) {
            if (codec.name().replace('_', '-').equalsIgnoreCase(normalized)) {
                return codec;
            }
        }
        throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Unsupported COMPRESSION: " + mode);
    }

    InputStream wrap(InputStream input) throws IOException {
        if (this == NONE) {
            return input;
        }
        var buffered = new BufferedInputStream(input, BUFFER_SIZE);
        buffered.mark(1);
        if (buffered.read() < 0) {
            throw new IOException("Missing compressed stream header");
        }
        buffered.reset();
        try {
            return new BufferedInputStream(FACTORY.createCompressorInputStream(format, buffered), BUFFER_SIZE);
        }
        catch (CompressorException e) {
            if (e.getCause() instanceof IOException failure) {
                throw failure;
            }
            throw new IOException(e.getMessage(), e);
        }
    }

    long bufferRetainedSizeInBytes() {
        return this == NONE ? 0 : BUFFER_RETAINED_SIZE;
    }
}
