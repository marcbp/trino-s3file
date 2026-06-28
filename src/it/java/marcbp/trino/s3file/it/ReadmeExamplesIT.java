package marcbp.trino.s3file.it;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReadmeExamplesIT {
    private static TrinoIntegrationEnvironment environment;

    @BeforeAll
    static void setUp() throws Exception {
        environment = TrinoIntegrationEnvironment.create();
        environment.awaitReadiness();
        environment.seedSampleData();
    }

    @AfterAll
    static void tearDown() {
        if (environment != null) {
            environment.close();
        }
    }

    @Test
    void listBucketsMatchesReadmeStyle() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT path, bucket
                FROM TABLE(s3file.list.buckets())
                ORDER BY bucket
                """);

        assertEquals(List.of(List.of("s3://mybucket", "mybucket")), rows);
    }

    @Test
    void listObjectsMatchesReadmeStyle() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT path, bucket, key, name, parent, size, etag, type
                FROM TABLE(
                    s3file.list.objects(
                        bucket => 'mybucket'
                    )
                )
                ORDER BY key
                """);

        assertTrue(rows.size() >= 4, "expected at least the README example rows");
        assertHasObjectRow(rows, "s3://mybucket/data.csv", "mybucket", "data.csv", "data.csv", "", "object");
        assertHasObjectRow(rows, "s3://mybucket/data.jsonl", "mybucket", "data.jsonl", "data.jsonl", "", "object");
        assertHasObjectRow(rows, "s3://mybucket/data.txt", "mybucket", "data.txt", "data.txt", "", "object");
        assertHasObjectRow(rows, "s3://mybucket/data.xml", "mybucket", "data.xml", "data.xml", "", "object");

        for (List<String> row : rows) {
            assertTrue(Long.parseLong(row.get(5)) > 0, "size should be positive");
            assertTrue(row.get(6) != null && !row.get(6).isBlank(), "etag should be present");
        }
    }

    @Test
    void loadJsonMatchesReadmeExample() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT *
                FROM TABLE(
                    s3file.json.load(
                        path => 's3://mybucket/data.jsonl',
                        schema_sample_rows => 100,
                        additional_columns => 'nickname:varchar'
                    )
                )
                ORDER BY CAST(id AS bigint)
                """);

        assertEquals(List.of(
                Arrays.asList("1", "André", "Merlaux", "25", "active", null),
                Arrays.asList("2", "Roger", "Moulinier", "46", "active", null),
                Arrays.asList("3", "Jacky", "Jacquard", "44", "active", null),
                Arrays.asList("4", "Jean-René", "Calot", "47", "active", null),
                Arrays.asList("5", "Georges", "Préjean", "67", "inactive", "Moïse")
        ), rows);
    }

    @Test
    void loadXmlMatchesReadmeExample() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT "@id", firstname, lastname, age, status, nickname
                FROM TABLE(
                    s3file.xml.load(
                        path => 's3://mybucket/data.xml',
                        row_element => 'employee',
                        empty_as_null => 'true',
                        invalid_row_column => '',
                        encoding => 'UTF-8'
                    )
                )
                ORDER BY CAST("@id" AS bigint)
                """);

        assertEquals(List.of(
                Arrays.asList("1", "André", "Merlaux", "25", "active", null),
                Arrays.asList("2", "Roger", "Moulinier", "46", "active", null),
                Arrays.asList("3", "Jacky", "Jacquard", "44", "active", null),
                Arrays.asList("4", "Jean-René", "Calot", "47", "active", null),
                Arrays.asList("5", "Georges", "Préjean", "67", "inactive", "Moïse")
        ), rows);
    }

    @Test
    void loadCsvMatchesReadmeExample() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT *
                FROM TABLE(
                    s3file.csv.load(
                        path => 's3://mybucket/data.csv',
                        delimiter => ';',
                        header => 'true',
                        encoding => 'UTF-8'
                    )
                )
                ORDER BY CAST(id AS bigint)
                """);

        assertEquals(List.of(
                Arrays.asList("1", "André", "Merlaux", "", "25", "active"),
                Arrays.asList("2", "Roger", "Moulinier", "", "46", "active"),
                Arrays.asList("3", "Jacky", "Jacquard", "", "44", "active"),
                Arrays.asList("4", "Jean-René", "Calot", "", "47", "active"),
                Arrays.asList("5", "Georges", "Préjean", "Moïse", "67", "inactive")
        ), rows);
    }

    @Test
    void loadTxtMatchesReadmeExample() throws Exception {
        List<List<String>> rows = environment.query("""
                WITH parsed AS (
                    SELECT split_to_map(line, ' ', '=') AS fields
                    FROM TABLE(
                        s3file.txt.load(
                            path => 's3://mybucket/data.txt',
                            line_break => '\\n',
                            encoding => 'UTF-8'
                        )
                    )
                )
                SELECT
                    CAST(element_at(fields, 'id') AS bigint) AS id,
                    element_at(fields, 'firstname') AS firstname,
                    UPPER(element_at(fields, 'lastname')) AS lastname,
                    CAST(element_at(fields, 'age') AS bigint) AS age,
                    element_at(fields, 'nickname') AS nickname,
                    element_at(fields, 'status') AS status
                FROM parsed
                ORDER BY id
                """);

        assertEquals(List.of(
                Arrays.asList("1", "André", "MERLAUX", "25", null, "active"),
                Arrays.asList("2", "Roger", "MOULINIER", "46", null, "active"),
                Arrays.asList("3", "Jacky", "JACQUARD", "44", null, "active"),
                Arrays.asList("4", "Jean-René", "CALOT", "47", null, "active"),
                Arrays.asList("5", "Georges", "PRÉJEAN", "67", "Moïse", "inactive")
        ), rows);
    }

    private static void assertHasObjectRow(List<List<String>> rows, String path, String bucket, String key, String name, String parent, String type) {
        boolean match = rows.stream().anyMatch(row ->
                path.equals(row.get(0))
                        && bucket.equals(row.get(1))
                        && key.equals(row.get(2))
                        && name.equals(row.get(3))
                        && parent.equals(row.get(4))
                        && type.equals(row.get(7)));
        assertTrue(match, () -> "missing row for " + path);
    }
}
