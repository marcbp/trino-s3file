package marcbp.trino.s3file.txt;

import io.airlift.slice.Slices;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.VarcharType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;

/**
 * Utilities for reading newline-delimited text records.
 */
public final class TextLines {
    private TextLines() {
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
