package marcbp.trino.s3file.csv;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * CSV-specific utilities shared across the connector.
 */
public final class CsvFormatSupport {
    private CsvFormatSupport() {}

    public static long calculateLineBytes(String value, Charset charset, byte[] lineBreakBytes) {
        return value.getBytes(charset).length + lineBreakBytes.length;
    }

    public static List<String> inferColumnNames(BufferedReader reader, String sourceDescription, char delimiter, boolean headerPresent) {
        return inferColumnNames(reader, sourceDescription, delimiter, headerPresent, false);
    }

    public static List<String> inferColumnNames(
            BufferedReader reader,
            String sourceDescription,
            char delimiter,
            boolean headerPresent,
            boolean multiline) {
        requireNonNull(reader, "reader is null");
        try {
            CSVParser parser = newParser(delimiter);
            String[] tokens = readRecord(reader, parser, multiline);
            if (tokens == null) {
                throw new IllegalArgumentException("CSV file is empty: " + sourceDescription);
            }
            List<String> columns = new ArrayList<>();
            if (headerPresent) {
                for (String token : tokens) {
                    if (token == null) {
                        continue;
                    }
                    String trimmed = token.trim();
                    if (!trimmed.isEmpty()) {
                        columns.add(trimmed);
                    }
                }
                if (columns.isEmpty()) {
                    throw new IllegalArgumentException("No column detected in CSV header: " + sourceDescription);
                }
            }
            else {
                for (int i = 0; i < tokens.length; i++) {
                    columns.add("column_" + (i + 1));
                }
                if (columns.isEmpty()) {
                    throw new IllegalArgumentException("Unable to infer column count from first row: " + sourceDescription);
                }
            }
            return List.copyOf(columns);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read CSV header: " + sourceDescription, e);
        }
    }

    private static String[] readRecord(BufferedReader reader, CSVParser parser, boolean multiline) throws IOException {
        List<String> values = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parsed = multiline ? parser.parseLineMulti(line) : parser.parseLine(line);
            if (parsed != null) {
                java.util.Collections.addAll(values, parsed);
            }
            if (!multiline || !parser.isPending()) {
                return values.toArray(String[]::new);
            }
        }
        if (parser.isPending()) {
            throw new IllegalArgumentException("Unterminated multiline CSV record");
        }
        return values.isEmpty() ? null : values.toArray(String[]::new);
    }

    public static CSVParser newParser(char delimiter) {
        return new CSVParserBuilder()
                .withSeparator(delimiter)
                .build();
    }

    public static String[] parseCsvLine(String line, char delimiter) {
        return parseCsvLine(line, newParser(delimiter));
    }

    public static String[] parseCsvLine(String line, CSVParser parser) {
        requireNonNull(parser, "parser is null");
        try {
            return parser.parseLine(line);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse CSV line: " + line, e);
        }
    }
}
