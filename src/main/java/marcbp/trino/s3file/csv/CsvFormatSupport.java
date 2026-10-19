package marcbp.trino.s3file.csv;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * CSV-specific utilities shared across the connector.
 */
public final class CsvFormatSupport {
    private CsvFormatSupport() {}

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
                return null;
            }
            List<String> columns = new ArrayList<>();
            if (headerPresent) {
                for (String token : tokens) {
                    String trimmed = token == null ? "" : token.trim();
                    if (trimmed.isEmpty() || columns.stream().anyMatch(name -> name.equalsIgnoreCase(trimmed))) {
                        throw new IllegalArgumentException("Blank or duplicate CSV column in " + sourceDescription);
                    }
                    columns.add(trimmed);
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
                Collections.addAll(values, parsed);
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
}
