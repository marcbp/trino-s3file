package marcbp.trino.s3file.txt;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarcharType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;

/**
 * Business logic utilities for interpreting plain-text formatting options and records.
 */
public final class TextFormatSupport {
    private TextFormatSupport() {
    }

    public static String decodeEscapes(String value) {
        StringBuilder result = new StringBuilder();
        boolean escaping = false;
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            if (escaping) {
                switch (ch) {
                    case 'n' -> result.append('\n');
                    case 'r' -> result.append('\r');
                    case 't' -> result.append('\t');
                    case '\\' -> result.append('\\');
                    default -> result.append(ch);
                }
                escaping = false;
            }
            else if (ch == '\\') {
                escaping = true;
            }
            else {
                result.append(ch);
            }
        }
        if (escaping) {
            result.append('\\');
        }
        return result.toString();
    }

    public static String formatForLog(String delimiter) {
        return delimiter
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    public static Optional<TextRecord> readNextLine(
            BufferedReader reader,
            Charset charset,
            String lineBreak,
            byte[] lineBreakBytes,
            long primaryLength,
            long bytesWithinPrimary,
            boolean lastSplit)
            throws IOException {
        Optional<DelimitedText> line = readUntilDelimiter(reader, lineBreak);
        if (line.isEmpty()) {
            return Optional.empty();
        }
        DelimitedText text = line.orElseThrow();
        long lineBytes = calculateLineBytes(text.value(), charset, lineBreakBytes, text.terminated());
        boolean finishesSplit = !lastSplit && bytesWithinPrimary + lineBytes > primaryLength;
        return Optional.of(new TextRecord(text.value(), lineBytes, finishesSplit));
    }

    public static long calculateLineBytes(String value, Charset charset, byte[] lineBreakBytes) {
        return calculateLineBytes(value, charset, lineBreakBytes, true);
    }

    public static long calculateLineBytes(String value, Charset charset, byte[] lineBreakBytes, boolean terminated) {
        long delimiterBytes = terminated ? lineBreakBytes.length : 0;
        return value.getBytes(charset).length + delimiterBytes;
    }

    private static Optional<DelimitedText> readUntilDelimiter(BufferedReader reader, String delimiter)
            throws IOException {
        StringBuilder value = new StringBuilder();
        int next;
        while ((next = reader.read()) != -1) {
            value.append((char) next);
            if (endsWith(value, delimiter)) {
                value.setLength(value.length() - delimiter.length());
                return Optional.of(new DelimitedText(value.toString(), true));
            }
        }
        if (value.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DelimitedText(value.toString(), false));
    }

    private static boolean endsWith(StringBuilder value, String suffix) {
        if (value.length() < suffix.length()) {
            return false;
        }
        int offset = value.length() - suffix.length();
        for (int i = 0; i < suffix.length(); i++) {
            if (value.charAt(offset + i) != suffix.charAt(i)) {
                return false;
            }
        }
        return true;
    }

    private record DelimitedText(String value, boolean terminated) {}

    public record TextRecord(String value, long bytes, boolean finishesSplit) {}

    public static void writeLine(BlockBuilder blockBuilder, VarcharType type, String value) {
        type.writeSlice(blockBuilder, Slices.utf8Slice(value));
    }
}
