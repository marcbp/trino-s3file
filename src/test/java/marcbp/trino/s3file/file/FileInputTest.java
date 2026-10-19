package marcbp.trino.s3file.file;

import io.trino.spi.TrinoException;
import marcbp.trino.s3file.s3.S3ClientBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class FileInputTest {
    @Test
    void retainedIoMemoryDoesNotDependOnCodecInternals() throws Exception {
        var retainedSizes = new HashSet<Long>();
        for (Compression codec : Compression.values()) {
            if (codec == Compression.NONE) {
                continue;
            }
            byte[] compressed = CompressedTestData.encode(codec, "content".getBytes(StandardCharsets.UTF_8));
            var client = mock(S3ClientBuilder.SessionClient.class);
            var object = new S3ObjectRef("s3://bucket/file", compressed.length, null, null, codec);
            when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty()))
                    .thenReturn(new ByteArrayInputStream(compressed));
            try (var input = FileInput.open(client, object, 0, null)) {
                input.readAllBytes();
                retainedSizes.add(input.getRetainedSizeInBytes());
            }
        }
        assertEquals(1, retainedSizes.size(), "All codecs retain the same connector-owned I/O buffers");
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void streamsBeforeEofCountsBothUnitsAndClosesItsSource(Compression codec) throws Exception {
        byte[] content = CompressedTestData.content("txt").getBytes(StandardCharsets.UTF_8);
        byte[] compressed = CompressedTestData.compressed(codec, "txt");
        var raw = spy(new ByteArrayInputStream(compressed));
        var client = mock(S3ClientBuilder.SessionClient.class);
        var object = new S3ObjectRef("s3://bucket/file", compressed.length, "etag", null, codec);
        when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.of("etag"))).thenReturn(raw);
        var input = FileInput.open(client, object, 0, null);
        assertArrayEquals(Arrays.copyOf(content, 1000), input.readNBytes(1000));
        assertEquals(1000, input.decodedBytes());
        assertTrue(input.physicalBytes() > 0 && input.physicalBytes() < compressed.length);
        long retained = input.getRetainedSizeInBytes();
        assertTrue(retained > 8192);
        input.close();
        input.close();
        assertTrue(input.getRetainedSizeInBytes() < retained);
        verify(raw, times(1)).close();
        assertTrue(raw.available() > 0, "Closing must not drain the compressed tail");
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = {"GZIP", "BZIP2", "ZSTD", "LZ4_FRAMED", "SNAPPY_FRAMED"})
    void concatenatedMembersAndEmptyMembersAreReadToTheEnd(Compression codec) throws Exception {
        var bytes = new ByteArrayOutputStream();
        bytes.write(CompressedTestData.encode(codec, new byte[0]));
        bytes.write(CompressedTestData.encode(codec, "été\n".getBytes(StandardCharsets.UTF_8)));
        bytes.write(CompressedTestData.encode(codec, "last".getBytes(StandardCharsets.UTF_8)));
        byte[] compressed = bytes.toByteArray();
        var client = mock(S3ClientBuilder.SessionClient.class);
        var object = new S3ObjectRef("s3://bucket/concat.gz", compressed.length, null, null, codec);
        when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty()))
                .thenReturn(new ByteArrayInputStream(compressed));
        try (var input = FileInput.open(client, object, 0, null)) {
            assertEquals("été\nlast", new String(input.readAllBytes(), StandardCharsets.UTF_8));
            assertEquals(compressed.length, input.physicalBytes());
            assertEquals("été\nlast".getBytes(StandardCharsets.UTF_8).length, input.decodedBytes());
        }
    }

    @Test
    void badHeadersTruncationAndCrcFailuresNameTheObjectAndCloseIt() throws Exception {
        byte[] valid = gzip("content".getBytes(StandardCharsets.UTF_8));
        byte[] badCrc = valid.clone();
        badCrc[badCrc.length - 8] ^= 1;
        for (byte[] bytes : new byte[][] {new byte[0], "not gzip".getBytes(StandardCharsets.UTF_8),
                Arrays.copyOf(valid, valid.length - 4), badCrc}) {
            var raw = spy(new ByteArrayInputStream(bytes));
            var client = mock(S3ClientBuilder.SessionClient.class);
            var object = new S3ObjectRef("s3://bucket/bad.gz", bytes.length, null, null, Compression.GZIP);
            when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty())).thenReturn(raw);
            var failure = assertThrows(TrinoException.class, () -> {
                try (var input = FileInput.open(client, object, 0, null)) {
                    input.readAllBytes();
                }
            });
            assertTrue(failure.getMessage().contains(object.path()));
            assertEquals("INVALID_FUNCTION_ARGUMENT", failure.getErrorCode().getName());
            verify(raw, times(1)).close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void invalidCompressedContentNamesTheCodecAndObject(Compression codec) throws Exception {
        for (byte[] bytes : new byte[][] {new byte[0], "not compressed".getBytes(StandardCharsets.UTF_8)}) {
            var raw = spy(new ByteArrayInputStream(bytes));
            var client = mock(S3ClientBuilder.SessionClient.class);
            var object = new S3ObjectRef("s3://bucket/bad", bytes.length, null, null, codec);
            when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty())).thenReturn(raw);
            var failure = assertThrows(TrinoException.class, () -> {
                try (var input = FileInput.open(client, object, 0, null)) {
                    input.readAllBytes();
                }
            });
            assertTrue(failure.getMessage().contains(object.path()));
            assertTrue(failure.getMessage().contains(codec.name()));
            verify(raw, times(1)).close();
        }
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void transportFailuresAreNotReportedAsInvalidCompressedContent(Compression codec) throws Exception {
        var failure = new IOException("Connection reset");
        var raw = mock(InputStream.class);
        when(raw.read()).thenThrow(failure);
        when(raw.read(any(byte[].class), anyInt(), anyInt())).thenThrow(failure);
        var client = mock(S3ClientBuilder.SessionClient.class);
        var object = new S3ObjectRef("s3://bucket/interrupted", 1024, null, null, codec);
        when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty())).thenReturn(raw);
        assertSame(failure, assertThrows(IOException.class, () -> {
            try (var input = FileInput.open(client, object, 0, null)) {
                input.readAllBytes();
            }
        }));
        verify(raw, times(1)).close();
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void truncatedPayloadsFailInsteadOfReturningPartialResults(Compression codec) throws Exception {
        byte[] complete = CompressedTestData.compressed(codec, "txt");
        byte[] truncated = Arrays.copyOf(complete, complete.length / 2);
        var client = mock(S3ClientBuilder.SessionClient.class);
        var raw = spy(new ByteArrayInputStream(truncated));
        var object = new S3ObjectRef("s3://bucket/truncated", truncated.length, null, null, codec);
        when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty())).thenReturn(raw);
        var failure = assertThrows(TrinoException.class, () -> {
            try (var input = FileInput.open(client, object, 0, null)) {
                input.readAllBytes();
            }
        });
        assertTrue(failure.getMessage().contains(object.path()));
        verify(raw, times(1)).close();
    }

    @ParameterizedTest
    @EnumSource(value = Compression.class, names = "NONE", mode = EnumSource.Mode.EXCLUDE)
    void validCompressedEmptyFilesAreEmpty(Compression codec) throws Exception {
        byte[] bytes = CompressedTestData.encode(codec, new byte[0]);
        var client = mock(S3ClientBuilder.SessionClient.class);
        var object = new S3ObjectRef("s3://bucket/empty", bytes.length, null, null, codec);
        when(client.openStream(object.path(), 0, null, Optional.empty(), Optional.empty()))
                .thenReturn(new ByteArrayInputStream(bytes));
        try (var input = FileInput.open(client, object, 0, null)) {
            assertEquals(-1, input.read());
            assertEquals(0, input.decodedBytes());
            assertEquals(bytes.length, input.physicalBytes());
        }
    }

    static byte[] gzip(byte[] content) throws Exception {
        return CompressedTestData.encode(Compression.GZIP, content);
    }
}
