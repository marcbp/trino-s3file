package marcbp.trino.s3file.file;

import org.apache.commons.compress.compressors.CompressorStreamFactory;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

/** Shared streaming data for TU and IT. */
public final class CompressedTestData {
    public static final int ROWS = 20_000;

    private CompressedTestData() {}

    public static String content(String format) {
        var random = new Random(42);
        var content = new StringBuilder(switch (format) {
            case "xml" -> "<rows>";
            case "csv" -> "value\n";
            default -> "";
        });
        for (int i = 0; i < ROWS; i++) {
            // Keep compressed inputs larger than codec read-ahead buffers.
            byte[] bytes = new byte[96];
            random.nextBytes(bytes);
            String value = "été-" + Base64.getEncoder().encodeToString(bytes);
            content.append(switch (format) {
                case "txt", "csv" -> value + "\n";
                case "json" -> "{\"value\":\"" + value + "\"}\n";
                case "xml" -> "<row><value>" + value + "</value></row>";
                default -> throw new IllegalArgumentException(format);
            });
        }
        if (format.equals("xml")) {
            content.append("</rows>");
        }
        return content.toString();
    }

    public static byte[] compressed(Compression codec, String format) throws Exception {
        return encode(codec, content(format).getBytes(StandardCharsets.UTF_8));
    }

    static byte[] encode(Compression codec, byte[] content) throws Exception {
        String name = switch (codec) {
            case GZIP -> CompressorStreamFactory.GZIP;
            case BZIP2 -> CompressorStreamFactory.BZIP2;
            case ZSTD -> CompressorStreamFactory.ZSTANDARD;
            case LZ4_FRAMED -> CompressorStreamFactory.LZ4_FRAMED;
            case SNAPPY_FRAMED -> CompressorStreamFactory.SNAPPY_FRAMED;
            default -> throw new IllegalArgumentException("No encoder for " + codec);
        };
        var bytes = new ByteArrayOutputStream();
        try (var output = new CompressorStreamFactory().createCompressorOutputStream(name, bytes)) {
            output.write(content);
        }
        return bytes.toByteArray();
    }
}
