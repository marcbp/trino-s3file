package marcbp.trino.s3file.csv;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

import marcbp.trino.s3file.S3FileLogger;

import static java.util.Objects.requireNonNull;

/**
 * Encapsulates CSV parsing helpers used by the connector.
 */
public final class CsvProcessingService {
    private static final S3FileLogger LOG = S3FileLogger.get(CsvProcessingService.class);

    public List<String> inferColumnNames(BufferedReader reader, String sourceDescription, char delimiter, boolean headerPresent) {
        requireNonNull(reader, "reader is null");
        try {
            String header = reader.readLine();
            LOG.info("Read header from {}: {}", sourceDescription, header);
            if (header == null) {
                throw new IllegalArgumentException("CSV file is empty: " + sourceDescription);
            }
            String[] tokens = parseCsvLine(header, delimiter);
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
                LOG.info("Header disabled for {} ; generating default column names", sourceDescription);
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
            LOG.error("Failed to read CSV header for {}", sourceDescription, e);
            throw new UncheckedIOException("Failed to read CSV header: " + sourceDescription, e);
        }
    }

    public String[] parseCsvLine(String line, char delimiter) {
        CSVParser parser = new CSVParserBuilder()
                .withSeparator(delimiter)
                .build();
        try {
            String[] parsed = parser.parseLine(line);
            LOG.debug("Parsed line with {} tokens", parsed.length);
            return parsed;
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse CSV line: " + line, e);
        }
    }
}
