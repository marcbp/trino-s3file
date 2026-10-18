package marcbp.trino.s3file.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache5.Apache5HttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.BucketAlreadyOwnedByYouException;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

final class TrinoIntegrationEnvironment implements AutoCloseable {
    private static final String DEFAULT_TRINO_BASE_URL = "http://trino-coordinator:8080";
    private static final String DEFAULT_S3_ENDPOINT = "http://seaweedfs:9000";
    private static final String BUCKET = "mybucket";
    private static final String S3_ACCESS_KEY = "s3file";
    private static final String S3_SECRET_KEY = "s3file123";
    private static final Duration READY_TIMEOUT = Duration.ofMinutes(2);

    private static final String CSV_DATA = """
            id;firstname;lastname;nickname;age;status
            1;André;Merlaux;;25;active
            2;Roger;Moulinier;;46;active
            3;Jacky;Jacquard;;44;active
            4;Jean-René;Calot;;47;active
            5;Georges;Préjean;Moïse;67;inactive
            """;

    private static final String JSON_DATA = """
            {"id":1,"firstname":"André","lastname":"Merlaux","age":25,"status":"active"}
            {"id":2,"firstname":"Roger","lastname":"Moulinier","age":46,"status":"active"}
            {"id":3,"firstname":"Jacky","lastname":"Jacquard","age":44,"status":"active"}
            {"id":4,"firstname":"Jean-René","lastname":"Calot","age":47,"status":"active"}
            {"id":5,"firstname":"Georges","lastname":"Préjean","nickname":"Moïse","age":67,"status":"inactive"}
            """;

    private static final String TXT_DATA = """
            id=1 firstname=André lastname=Merlaux age=25 status=active
            id=2 firstname=Roger lastname=Moulinier age=46 status=active
            id=3 firstname=Jacky lastname=Jacquard age=44 status=active
            id=4 firstname=Jean-René lastname=Calot age=47 status=active
            id=5 firstname=Georges lastname=Préjean nickname=Moïse age=67 status=inactive
            """;

    private static final String XML_DATA = """
            <employees>
              <employee id="1">
                <firstname>André</firstname>
                <lastname>Merlaux</lastname>
                <age>25</age>
                <status>active</status>
              </employee>
              <employee id="2">
                <firstname>Roger</firstname>
                <lastname>Moulinier</lastname>
                <age>46</age>
                <status>active</status>
              </employee>
              <employee id="3">
                <firstname>Jacky</firstname>
                <lastname>Jacquard</lastname>
                <age>44</age>
                <status>active</status>
              </employee>
              <employee id="4">
                <firstname>Jean-René</firstname>
                <lastname>Calot</lastname>
                <age>47</age>
                <status>active</status>
              </employee>
              <employee id="5">
                <firstname>Georges</firstname>
                <lastname>Préjean</lastname>
                <nickname>Moïse</nickname>
                <age>67</age>
                <status>inactive</status>
              </employee>
            </employees>
            """;

    private final URI trinoBaseUri;
    private final URI s3Endpoint;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final S3Client s3Client;

    private TrinoIntegrationEnvironment(URI trinoBaseUri, URI s3Endpoint) {
        this.trinoBaseUri = trinoBaseUri;
        this.s3Endpoint = s3Endpoint;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5))
                .build();
        this.objectMapper = new ObjectMapper();
        this.s3Client = createS3Client();
    }

    static TrinoIntegrationEnvironment create() {
        URI trinoBaseUri = URI.create(setting("trino.base-url", "TRINO_BASE_URL", DEFAULT_TRINO_BASE_URL));
        URI s3Endpoint = URI.create(setting("s3.endpoint", "S3_ENDPOINT", DEFAULT_S3_ENDPOINT));
        return new TrinoIntegrationEnvironment(trinoBaseUri, s3Endpoint);
    }

    void awaitReadiness() throws InterruptedException {
        awaitHttpOk(trinoBaseUri.resolve("/v1/info"), "Trino");
        awaitHttpOk(s3Endpoint.resolve("/status"), "SeaweedFS");
    }

    void seedSampleData() {
        createBucket(BUCKET);
        putObject("data.csv", CSV_DATA);
        putObject("data.jsonl", JSON_DATA);
        putObject("data.txt", TXT_DATA);
        putObject("data.xml", XML_DATA);
        putObject("listing/direct.txt", "direct\n");
        putObject("listing/nested/child.txt", "nested\n");
        putObject("malformed.jsonl", "{\"id\":1}\nnot-json\n");
        putObject("split-data.txt", largeTextData());
        putObject("split-data.jsonl", largeJsonData());
        putObject("split-data.csv", largeCsvData());
        for (String prefix : List.of("multi/a", "multi/nested/b")) {
            putObject(prefix + ".txt", largeTextData());
            putObject(prefix + ".csv", largeCsvData());
            putObject(prefix + ".jsonl", largeJsonData());
        }
        putObject("schema/00-empty.jsonl", "");
        putObject("schema/01-reference.jsonl", "{\"id\":1,\"optional\":\"yes\"}\n");
        putObject("schema/02-missing.jsonl", "{\"id\":2}\n");
        putObject("bad-type/01.jsonl", "{\"id\":1}\n");
        putObject("bad-type/02.jsonl", "{\"id\":1.5}\n");
        putObject("bad-field/01.jsonl", "{\"id\":1}\n");
        putObject("bad-field/02.jsonl", "{\"id\":2,\"extra\":true}\n");
        putObject("bad-header/01.csv", "id;name\n1;one\n");
        putObject("bad-header/02.csv", "name;id\ntwo;2\n");
        putObject("bad-width/01.csv", "id;name\n1;one\n");
        putObject("bad-width/02.csv", "id;name\n2\n");
        putObject("xml-multi/00.xml", "<items/>");
        putObject("xml-multi/01.xml", "<items><row id=\"1\"><name>one</name></row></items>");
        putObject("xml-multi/02.xml", "<items><row id=\"2\"/></items>");
        putObject("bad-xml/01.xml", "<items><row><name>one</name></row></items>");
        putObject("bad-xml/02.xml", "<items><row><extra>two</extra></row></items>");
        putObject("multi-line/01.csv", "id;comment\n1;\"hello\nworld\"\n");
        putObject("multi-line/02.csv", "id;comment\n2;plain\n");
        for (String value : List.of("literal", "neighbor")) {
            String key = value.equals("literal") ? "escaped/file[2026]*?" : "escaped/file2-other";
            putObject(key + ".txt", value + "\n");
            putObject(key + ".csv", "value\n" + value + "\n");
            putObject(key + ".jsonl", "{\"value\":\"" + value + "\"}\n");
            putObject(key + ".xml", "<rows><row><value>" + value + "</value></row></rows>");
        }
        putObject("multiline.csv", "id;comment\n1;\"hello\nfrom two physical lines\"\n2;plain\n");
    }

    List<List<String>> query(String sql) throws IOException, InterruptedException {
        JsonNode response = executeSql(sql);
        List<List<String>> rows = new ArrayList<>();

        while (true) {
            JsonNode error = response.get("error");
            if (error != null && !error.isNull()) {
                throw new AssertionError("Trino query failed for SQL:\n" + sql + "\n\n" + response);
            }

            JsonNode data = response.get("data");
            if (data != null && data.isArray()) {
                for (JsonNode row : data) {
                    List<String> values = new ArrayList<>(row.size());
                    for (JsonNode value : row) {
                        values.add(value.isNull() ? null : value.asText());
                    }
                    rows.add(values);
                }
            }

            JsonNode nextUri = response.get("nextUri");
            if (nextUri == null || nextUri.isNull()) {
                return rows;
            }

            response = getJson(URI.create(nextUri.asText()));
        }
    }

    @Override
    public void close() {
        s3Client.close();
    }

    private JsonNode executeSql(String sql) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(trinoBaseUri.resolve("/v1/statement"))
                .timeout(Duration.ofMinutes(2))
                .header("Content-Type", "text/plain; charset=utf-8")
                .header("X-Trino-User", "integration-tests")
                .header("X-Trino-Source", "integration-tests")
                .header("X-Trino-Time-Zone", "UTC")
                .POST(HttpRequest.BodyPublishers.ofString(sql))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new AssertionError("Unexpected Trino status " + response.statusCode() + " for SQL:\n" + sql + "\n\n" + response.body());
        }
        return objectMapper.readTree(response.body());
    }

    private JsonNode getJson(URI uri) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofMinutes(2))
                .header("X-Trino-User", "integration-tests")
                .header("X-Trino-Source", "integration-tests")
                .header("X-Trino-Time-Zone", "UTC")
                .GET()
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new AssertionError("Unexpected Trino status " + response.statusCode() + " while polling " + uri + "\n\n" + response.body());
        }
        return objectMapper.readTree(response.body());
    }

    private void awaitHttpOk(URI uri, String serviceName) throws InterruptedException {
        long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadline) {
            try {
                HttpRequest request = HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(5))
                        .GET()
                        .build();
                HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    return;
                }
                lastFailure = new IllegalStateException(serviceName + " returned HTTP " + response.statusCode());
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (Exception e) {
                lastFailure = e;
            }

            Thread.sleep(1_000L);
        }

        throw new IllegalStateException(format("Timed out waiting for %s at %s", serviceName, uri), lastFailure);
    }

    private S3Client createS3Client() {
        return S3Client.builder()
                .endpointOverride(s3Endpoint)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3_ACCESS_KEY, S3_SECRET_KEY)))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .httpClientBuilder(Apache5HttpClient.builder())
                .build();
    }

    private void createBucket(String bucket) {
        try {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        }
        catch (BucketAlreadyOwnedByYouException | BucketAlreadyExistsException ignored) {
            // Reuse the bucket across repeated local runs.
        }
    }

    private void putObject(String key, String body) {
        s3Client.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
                RequestBody.fromString(body, StandardCharsets.UTF_8));
    }

    private static String largeTextData() {
        String payload = "t".repeat(96);
        StringBuilder data = new StringBuilder(3 * 1024 * 1024);
        for (int id = 1; id <= 25_000; id++) {
            data.append(id).append('|').append(payload).append('\n');
        }
        return data.toString();
    }

    private static String largeJsonData() {
        String payload = "j".repeat(96);
        StringBuilder data = new StringBuilder(4 * 1024 * 1024);
        for (int id = 1; id <= 25_000; id++) {
            data.append("{\"id\":").append(id)
                    .append(",\"payload\":\"").append(payload)
                    .append("\"}\n");
        }
        return data.toString();
    }

    private static String largeCsvData() {
        String payload = "c".repeat(96);
        StringBuilder data = new StringBuilder(3 * 1024 * 1024).append("id;payload\n");
        for (int id = 1; id <= 25_000; id++) {
            data.append(id).append(';').append(payload).append('\n');
        }
        return data.toString();
    }

    private static String setting(String propertyName, String envName, String fallback) {
        String value = System.getProperty(propertyName);
        if (value != null && !value.isBlank()) {
            return value.trim();
        }

        value = System.getenv(envName);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return value.trim();
    }
}
