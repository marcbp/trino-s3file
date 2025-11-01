package marcbp.trino.s3file.txt;

/**
 * Business logic utilities for interpreting plain-text formatting options.
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
}
