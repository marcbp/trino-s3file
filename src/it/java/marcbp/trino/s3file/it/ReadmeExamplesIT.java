package marcbp.trino.s3file.it;

import marcbp.trino.s3file.file.CompressedTestData;
import marcbp.trino.s3file.file.Compression;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

        assertTrue(rows.contains(List.of("s3://mybucket", "mybucket")));
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

    @Test
    void tinySplitsReadEveryTextRecordExactlyOnce() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT count(*), count(DISTINCT split_part(line, '|', 1)),
                       min(CAST(split_part(line, '|', 1) AS bigint)),
                       max(CAST(split_part(line, '|', 1) AS bigint)),
                       sum(CAST(split_part(line, '|', 1) AS bigint))
                FROM TABLE(s3file.txt.load(
                    path => 's3://mybucket/split-data.txt',
                    split_size_mb => 1
                ))
                """);

        assertEquals(List.of(List.of("25000", "25000", "1", "25000", "312512500")), rows);
    }

    @Test
    void tinySplitsReadEveryJsonRecordExactlyOnce() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT count(*), count(DISTINCT id), min(id), max(id), sum(id)
                FROM TABLE(s3file.json.load(
                    path => 's3://mybucket/split-data.jsonl',
                    split_size_mb => 1,
                    schema_sample_rows => 1
                ))
                """);

        assertEquals(List.of(List.of("25000", "25000", "1", "25000", "312512500")), rows);
    }

    @Test
    void tinySplitsReadEveryCsvRecordExactlyOnce() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT count(*), count(DISTINCT id), min(CAST(id AS bigint)), max(CAST(id AS bigint)), sum(CAST(id AS bigint))
                FROM TABLE(s3file.csv.load(
                    path => 's3://mybucket/split-data.csv',
                    header => 'true',
                    multiline => 'false',
                    split_size_mb => 1
                ))
                """);

        assertEquals(List.of(List.of("25000", "25000", "1", "25000", "312512500")), rows);
    }

    @Test
    void multilineCsvRemainsCorrectWithTinyRequestedSplit() throws Exception {
        List<List<String>> rows = environment.query("""
                SELECT id, comment
                FROM TABLE(s3file.csv.load(
                    path => 's3://mybucket/multiline.csv',
                    header => 'true',
                    multiline => 'true',
                    split_size_mb => 1
                ))
                ORDER BY CAST(id AS bigint)
                """);

        assertEquals(List.of(
                List.of("1", "hello\nfrom two physical lines"),
                List.of("2", "plain")), rows);
    }

    @Test
    void jsonProjectionPreservesColumnOrderAndFilter() throws Exception {
        assertEquals(List.of(List.of("Moïse", "67")), environment.query("""
                SELECT nickname, age
                FROM TABLE(s3file.json.load(path => 's3://mybucket/data.jsonl'))
                WHERE age > 50
                """));
    }

    @Test
    void nonRecursiveListingReturnsImmediateObjectsAndPrefixes() throws Exception {
        assertEquals(List.of(
                List.of("listing/direct.txt", "object"),
                List.of("listing/nested/", "prefix")), environment.query("""
                SELECT key, type
                FROM TABLE(s3file.list.objects(
                    bucket => 'mybucket', prefix => 'listing/',
                    recursive_listing => 'false', include_prefixes => 'true'
                ))
                ORDER BY key
                """));
        assertEquals(List.of(List.of("0")), environment.query("""
                SELECT count(*) FROM TABLE(s3file.list.objects(
                    bucket => 'mybucket', prefix => 'listing/missing/'
                ))
                """));
    }

    @Test
    void countStillRejectsMalformedJsonAfterSchemaSample() {
        AssertionError error = assertThrows(AssertionError.class, () -> environment.query("""
                SELECT count(*) FROM TABLE(s3file.json.load(
                    path => 's3://mybucket/malformed.jsonl', schema_sample_rows => 1
                ))
                """));
        assertTrue(error.getMessage().contains("Invalid JSON"), error.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "path => ''",
            "path => 's3://mybucket/data.txt', compression => 'zip'",
            "path => 's3://mybucket/data.txt', split_size_mb => 0",
            "path => 's3://mybucket/data.txt', encoding => 'not-a-charset'"
    })
    void invalidArgumentsProduceSqlErrors(String arguments) {
        AssertionError error = assertThrows(AssertionError.class, () -> environment.query(
                "SELECT * FROM TABLE(s3file.txt.load(" + arguments + "))"));
        assertTrue(error.getMessage().contains("INVALID_FUNCTION_ARGUMENT"), error.getMessage());
    }

    @Test
    void missingS3ObjectFailsQueryWithoutBreakingSubsequentReads() throws Exception {
        AssertionError error = assertThrows(AssertionError.class, () -> environment.query("""
                SELECT * FROM TABLE(s3file.txt.load(path => 's3://mybucket/missing-object.txt'))
                """));
        assertTrue(error.getMessage().contains("Status Code: 404"), error.getMessage());
        assertEquals(List.of(List.of("5")), environment.query("""
                SELECT count(*) FROM TABLE(s3file.txt.load(path => 's3://mybucket/data.txt'))
                """));
    }

    @ParameterizedTest
    @ValueSource(strings = {"txt", "csv", "json"})
    void globKeepsMultipleSplitsPerObjectAndProvenance(String format) throws Exception {
        String extension = format.equals("json") ? "jsonl" : format;
        String id = format.equals("txt") ? "CAST(split_part(line, '|', 1) AS bigint)" : "CAST(id AS bigint)";
        var rows = environment.query("SELECT \"$path\", count(*), count(DISTINCT " + id + "), sum(" + id + ") "
                + "FROM TABLE(s3file." + format + ".load(path => 's3://mybucket/multi/{,**/}*." + extension
                + "', split_size_mb => 1)) GROUP BY 1 ORDER BY 1");
        assertEquals(List.of(
                List.of("s3://mybucket/multi/a." + extension, "25000", "25000", "312512500"),
                List.of("s3://mybucket/multi/nested/b." + extension, "25000", "25000", "312512500")), rows);
    }

    @Test
    void schemaComesFromFirstNonEmptyFileAndMissingValuesAreNull() throws Exception {
        assertEquals(List.of(List.of("1", "yes"), Arrays.asList("2", null)), environment.query("""
                SELECT id, optional FROM TABLE(s3file.json.load(
                    path => 's3://mybucket/schema/*.jsonl', path_column => ''
                )) ORDER BY id
                """));
        assertEquals(List.of(List.of("s3://mybucket/schema/01-reference.jsonl"),
                List.of("s3://mybucket/schema/02-missing.jsonl")), environment.query("""
                SELECT source FROM TABLE(s3file.json.load(
                    path => 's3://mybucket/schema/*.jsonl', path_column => 'source'
                )) ORDER BY source
                """));
    }

    @Test
    void globSupportsMultilineCsvAndXmlWithMissingFields() throws Exception {
        assertEquals(List.of(List.of("1", "hello\nworld"), List.of("2", "plain")), environment.query("""
                SELECT id, comment FROM TABLE(s3file.csv.load(
                    path => 's3://mybucket/multi-line/*.csv', multiline => 'true'
                )) ORDER BY id
                """));
        assertEquals(List.of(List.of("1", "one"), Arrays.asList("2", null)), environment.query("""
                SELECT "@id", name FROM TABLE(s3file.xml.load(
                    path => 's3://mybucket/xml-multi/*.xml', invalid_row_column => ''
                )) ORDER BY "@id"
                """));
    }

    @ParameterizedTest
    @ValueSource(strings = {"json:bad-type:jsonl", "json:bad-field:jsonl", "csv:bad-header:csv", "csv:bad-width:csv", "xml:bad-xml:xml"})
    void countRejectsIncompatibleFilesWithoutProjection(String scenario) {
        String[] parts = scenario.split(":");
        AssertionError error = assertThrows(AssertionError.class, () -> environment.query(
                "SELECT count(*) FROM TABLE(s3file." + parts[0] + ".load(path => 's3://mybucket/"
                        + parts[1] + "/*." + parts[2] + "'" + (parts[0].equals("xml") ? ", invalid_row_column => ''" : "") + "))"));
        assertTrue(error.getMessage().contains("schema") || error.getMessage().contains("JSON")
                || error.getMessage().contains("XML"), error.getMessage());
    }

    @Test
    void emptyGlobAndPathColumnCollisionFailClearly() {
        for (String arguments : List.of("path => 's3://mybucket/no-matches/*.txt'",
                "path => 's3://mybucket/multi/*.txt', path_column => 'line'")) {
            AssertionError error = assertThrows(AssertionError.class, () -> environment.query(
                    "SELECT * FROM TABLE(s3file.txt.load(" + arguments + "))"));
            assertTrue(error.getMessage().contains("INVALID_FUNCTION_ARGUMENT"), error.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"txt", "csv", "json", "xml"})
    void literalKeysCanBeReadByDisablingGlobOrEscaping(String format) throws Exception {
        String extension = format.equals("json") ? "jsonl" : format;
        String column = format.equals("txt") ? "line" : "value";
        String query = "SELECT " + column + " FROM TABLE(s3file." + format + ".load(path => 's3://mybucket/escaped/";
        assertEquals(List.of(List.of("literal")), environment.query(
                query + "file[2026]*?." + extension + "', glob => false))"));
        assertEquals(List.of(List.of("literal")), environment.query(
                query + "file\\[2026\\]\\*\\?." + extension + "'))"));
        assertEquals(List.of(List.of("neighbor")), environment.query(
                query + "file[2026]*?." + extension + "', glob => true))"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"txt", "json", "csv", "xml"})
    void gzipGlobReadsEveryFileAndPreservesItsPath(String format) throws Exception {
        String extension = format.equals("json") ? "jsonl" : format;
        String id = format.equals("txt") ? "CAST(split_part(line, '|', 1) AS bigint)" : "CAST(id AS bigint)";
        String splitSize = format.equals("xml") ? "" : ", split_size_mb => 1";
        var rows = environment.query("SELECT \"$path\", count(*), count(DISTINCT " + id + "), sum(" + id + ") "
                + "FROM TABLE(s3file." + format + ".load(path => 's3://mybucket/gzip/" + format + "/*." + extension
                + ".gz'" + splitSize + ")) GROUP BY 1 ORDER BY 1");
        assertEquals(IntStream.range(0, 24).mapToObj(part -> List.of(
                "s3://mybucket/gzip/" + format + "/part-%02d.".formatted(part) + extension + ".gz",
                "25000", "25000", "312512500")).toList(), rows);
    }

    @ParameterizedTest
    @ValueSource(strings = {"txt", "json", "csv", "xml"})
    void compressionAutoMixedFilesAndExplicitModes(String format) throws Exception {
        String expression = format.equals("txt") ? "line" : "value";
        assertEquals(List.of(List.of("2")), environment.query("SELECT count(*) FROM TABLE(s3file." + format
                + ".load(path => 's3://mybucket/gzip/mixed/" + format + "/*'))"));
        for (String mode : List.of("gzip", "none")) {
            String suffix = mode.equals("gzip") ? "forced/" + format + "/payload" : "plain/" + format + "/literal.gz";
            String expected = format.equals("txt") ? "1|été" : "été";
            assertEquals(List.of(List.of(expected)), environment.query("SELECT " + expression + " FROM TABLE(s3file."
                    + format + ".load(path => 's3://mybucket/gzip/" + suffix + "', compression => '" + mode + "'))"));
        }
    }

    @Test
    void gzipMultilineCsvStillStreamsLogicalRecords() throws Exception {
        assertEquals(List.of(List.of("1", "hello\nworld"), List.of("2", "plain")), environment.query("""
                SELECT id, comment FROM TABLE(s3file.csv.load(
                    path => 's3://mybucket/gzip/multiline.csv.gz', multiline => 'true')) ORDER BY id
                """));
    }

    @ParameterizedTest
    @ValueSource(strings = {"truncated", "crc", "header"})
    void invalidGzipFailsWithTheObjectPath(String kind) {
        String path = "s3://mybucket/gzip/errors/" + kind + ".gz";
        var error = assertThrows(AssertionError.class, () -> environment.query(
                "SELECT count(*) FROM TABLE(s3file.txt.load(path => '" + path + "'))"));
        assertTrue(error.getMessage().contains("INVALID_FUNCTION_ARGUMENT"), error.getMessage());
        assertTrue(error.getMessage().contains(path), error.getMessage());
    }

    static Stream<Arguments> codecsAndFormats() {
        return Arrays.stream(Compression.values()).filter(codec -> codec != Compression.NONE)
                .flatMap(codec -> Stream.of("txt", "csv", "json", "xml").map(format -> Arguments.of(codec, format)));
    }

    @ParameterizedTest
    @MethodSource("codecsAndFormats")
    void streamCodecsReadMultipleFilesWithAutoAndExplicitModes(Compression codec, String format) throws Exception {
        String prefix = "s3://mybucket/codecs/" + format + "/" + codec.name().toLowerCase(Locale.ROOT) + "/";
        String value = format.equals("txt") ? "line" : "value";
        String splitSize = format.equals("xml") ? "" : ", split_size_mb => 1";
        var rows = environment.query("SELECT \"$path\", count(*), count(DISTINCT " + value + ") FROM TABLE(s3file."
                + format + ".load(path => '" + prefix + "part-*." + TrinoIntegrationEnvironment.compressionExtension(codec)
                + "'" + splitSize + ")) GROUP BY 1 ORDER BY 1");
        String count = Integer.toString(CompressedTestData.ROWS);
        assertEquals(List.of(List.of(prefix + "part-01." + TrinoIntegrationEnvironment.compressionExtension(codec), count, count),
                List.of(prefix + "part-02." + TrinoIntegrationEnvironment.compressionExtension(codec), count, count)), rows);
        String mode = codec.name().toLowerCase(Locale.ROOT).replace('_', '-');
        assertEquals(List.of(List.of(count)), environment.query("SELECT count(*) FROM TABLE(s3file." + format
                + ".load(path => '" + prefix + "extensionless', compression => '" + mode + "'))"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"txt", "csv", "json", "xml"})
    void aGlobCanMixAllCompressionAlgorithms(String format) throws Exception {
        assertEquals(List.of(List.of(Integer.toString(2 * 5 * CompressedTestData.ROWS), "10")), environment.query(
                "SELECT count(*), count(DISTINCT \"$path\") FROM TABLE(s3file." + format
                        + ".load(path => 's3://mybucket/codecs/" + format + "/*/part-*'))"));
    }

    private static void assertHasObjectRow(List<List<String>> rows, String path, String bucket, String key, String name, String parent, String type) {
        boolean match = rows.stream().anyMatch(row ->
                path.equals(row.get(0))
                        && bucket.equals(row.get(1))
                        && key.equals(row.get(2))
                        && name.equals(row.get(3))
                        && parent.equals(row.get(4))
                        && Long.parseLong(row.get(5)) > 0
                        && row.get(6) != null
                        && !row.get(6).isBlank()
                        && type.equals(row.get(7)));
        assertTrue(match, () -> "missing row for " + path);
    }
}
