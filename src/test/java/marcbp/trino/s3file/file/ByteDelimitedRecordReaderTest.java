package marcbp.trino.s3file.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ByteDelimitedRecordReaderTest {
    @ParameterizedTest
    @ValueSource(strings = {"UTF-16", "UTF-16LE", "UTF-16BE"})
    void readsUnicodeLinesIncludingBomEncoding(String encoding) throws IOException {
        assertLines(encoding, "é\n終\n", List.of("é", "終"), false);
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTF-8", "UTF-16LE", "UTF-16BE"})
    void trimsCarriageReturnBeforeEncodedLineFeed(String encoding) throws IOException {
        assertLines(encoding, "é\r\n終\r\n", List.of("é", "終"), true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"UTF-16LE", "UTF-16BE"})
    void delimiterBytesInsideCharactersDoNotTerminateRecords(String encoding) throws IOException {
        // Both strings contain the encoded LF bytes across two code units, at an odd byte offset.
        String value = encoding.equals("UTF-16LE") ? "\u0a41\u4200" : "\u4100\u0a42";
        assertLines(encoding, value + "\nlast", List.of(value, "last"), false);
    }

    @Test
    void littleEndianBomAppliesToEveryRecord() throws IOException {
        byte[] data = "\ufeffé\r\n終\r\n".getBytes(StandardCharsets.UTF_16LE);
        assertLines(StandardCharsets.UTF_16, data, List.of("é", "終"), true);
    }

    private static void assertLines(String encoding, String content, List<String> expected, boolean trimCr) throws IOException {
        Charset charset = Charset.forName(encoding);
        byte[] data = content.getBytes(charset);
        assertLines(charset, data, expected, trimCr);
    }

    private static void assertLines(Charset charset, byte[] data, List<String> expected, boolean trimCr) throws IOException {
        List<String> actual = new ArrayList<>();
        long consumed = 0;
        try (var reader = new ByteDelimitedRecordReader(new ByteArrayInputStream(data), charset,
                "\n".getBytes(charset), trimCr)) {
            var record = reader.readNext();
            while (record.isPresent()) {
                actual.add(record.orElseThrow().value(charset));
                consumed += record.orElseThrow().bytesConsumed();
                record = reader.readNext();
            }
        }
        assertEquals(data.length, consumed, "all encoded bytes must be accounted for");
        assertEquals(expected, actual);
    }
}
