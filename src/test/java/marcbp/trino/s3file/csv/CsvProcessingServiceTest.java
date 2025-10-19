package marcbp.trino.s3file.csv;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvProcessingServiceTest {
    private final CsvProcessingService service = new CsvProcessingService();

    @Test
    void inferColumnNamesWithHeader() {
        BufferedReader reader = new BufferedReader(new StringReader(" first ; second ; third \n"));

        List<String> columns = service.inferColumnNames(reader, "test.csv", ';', true);

        assertEquals(List.of("first", "second", "third"), columns);
    }

    @Test
    void inferColumnNamesWithoutHeaderGeneratesDefaults() {
        BufferedReader reader = new BufferedReader(new StringReader("value1;value2;value3\n"));

        List<String> columns = service.inferColumnNames(reader, "test.csv", ';', false);

        assertEquals(List.of("column_1", "column_2", "column_3"), columns);
    }

    @Test
    void parseCsvLineHandlesQuotedDelimiter() {
        String[] tokens = service.parseCsvLine("\"value;inside\";plain", ';');

        assertArrayEquals(new String[]{"value;inside", "plain"}, tokens);
    }
}
