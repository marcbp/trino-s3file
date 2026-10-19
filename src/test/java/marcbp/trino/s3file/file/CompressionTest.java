package marcbp.trino.s3file.file;

import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class CompressionTest {
    @ParameterizedTest
    @CsvSource({"gzip,gz", "gzip,gzip", "bzip2,bz2", "bzip2,bzip2", "zstd,zst", "zstd,zstd",
            "lz4-framed,lz4", "snappy-framed,sz", "snappy-framed,snappy"})
    void resolvesExtensionsAndExplicitModes(String mode, String extension) {
        var codec = Compression.resolve(mode, "s3://bucket/extensionless");
        assertEquals(codec, Compression.resolve("auto", "s3://bucket/part." + extension));
        assertEquals(codec, Compression.resolve("auto", "s3://bucket/part." + extension.toUpperCase()));
        assertEquals(Compression.NONE, Compression.resolve("none", "s3://bucket/part." + extension));
    }

    @Test
    void resolvesAutoPerObjectAndSupportsExplicitOverrides() {
        assertEquals(Compression.GZIP, Compression.resolve("auto", "s3://bucket/part.jsonl.gz"));
        assertEquals(Compression.GZIP, Compression.resolve("auto", "s3://bucket/part.GZIP"));
        assertEquals(Compression.NONE, Compression.resolve("auto", "s3://bucket/part.jsonl"));
        assertEquals(Compression.GZIP, Compression.resolve(" GZIP ", "s3://bucket/extensionless"));
        assertEquals(Compression.NONE, Compression.resolve("none", "s3://bucket/literal.gz"));
        assertThrows(TrinoException.class, () -> Compression.resolve("zip", "s3://bucket/file.zip"));
    }
}
