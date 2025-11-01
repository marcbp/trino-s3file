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
            byte[] lineBreakBytes,
            long primaryLength,
            long bytesWithinPrimary,
            boolean lastSplit)
            throws IOException {
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                return Optional.empty();
            }
            long lineBytes = calculateLineBytes(line, charset, lineBreakBytes);
            boolean finishesSplit = !lastSplit && bytesWithinPrimary + lineBytes > primaryLength;
            return Optional.of(new TextRecord(line, lineBytes, finishesSplit));
        }
    }

    public static long calculateLineBytes(String value, Charset charset, byte[] lineBreakBytes) {
        return value.getBytes(charset).length + lineBreakBytes.length;
    }

    public static void writeLine(BlockBuilder blockBuilder, VarcharType type, String value) {
        type.writeSlice(blockBuilder, Slices.utf8Slice(value));
    }

    public record TextRecord(String value, long bytes, boolean finishesSplit) {}
}
