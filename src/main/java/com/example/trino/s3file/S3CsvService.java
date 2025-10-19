package com.example.trino.s3file;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.example.trino.s3file.S3FileLogger;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

public final class S3CsvService implements Closeable {
    private static final S3FileLogger LOG = S3FileLogger.get(S3CsvService.class);
    private static final Pattern S3_URI = Pattern.compile("s3://([^/]+)/(.+)");
    private final S3Client s3Client;

    public S3CsvService() {
        this(buildClientFromEnvironment());
    }

    public S3CsvService(S3Client s3Client) {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
    }

    public List<String> inferColumnNames(String s3Uri, char delimiter, boolean headerPresent) {
        LOG.info("Inferring schema for {}", s3Uri);
        try (BufferedReader reader = openReader(s3Uri)) {
            String header = reader.readLine();
            LOG.info("Read header: {}", header);
            if (header == null) {
                throw new IllegalArgumentException("CSV file is empty: " + s3Uri);
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
                    throw new IllegalArgumentException("No column detected in CSV header: " + s3Uri);
                }
            }
            else {
                LOG.info("Header disabled; generating default column names");
                for (int i = 0; i < tokens.length; i++) {
                    columns.add("column_" + (i + 1));
                }
                if (columns.isEmpty()) {
                    throw new IllegalArgumentException("Unable to infer column count from first row: " + s3Uri);
                }
            }
            return List.copyOf(columns);
        }
        catch (IOException e) {
            LOG.error("Failed to read CSV header {}", s3Uri, e);
            throw new UncheckedIOException("Failed to read CSV header: " + s3Uri, e);
        }
    }

    public BufferedReader openReader(String s3Uri) {
        S3Location location = parseLocation(s3Uri);
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(location.bucket())
                .key(location.key())
                .build();
        ResponseInputStream<GetObjectResponse> stream = s3Client.getObject(request);
        LOG.info("Opened S3 object {}/{}", location.bucket(), location.key());
        return new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
    }

    public String[] parseCsvLine(String line, char delimiter) {
        CSVParser parser = new CSVParserBuilder().withSeparator(delimiter).build();
        try {
            String[] parsed = parser.parseLine(line);
            LOG.debug("Parsed line with {} tokens", parsed.length);
            return parsed;
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse CSV line: " + line, e);
        }
    }

    private static S3Location parseLocation(String s3Uri) {
        Matcher matcher = S3_URI.matcher(s3Uri);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid S3 URI: " + s3Uri);
        }
        return new S3Location(matcher.group(1), matcher.group(2));
    }

    private static S3Client buildClientFromEnvironment() {
        Map<String, String> env = System.getenv();
        S3ClientBuilder builder = S3Client.builder();

        String regionValue = env.getOrDefault("AWS_REGION", "us-east-1");
        builder.region(Region.of(regionValue));

        String endpoint = env.get("AWS_S3_ENDPOINT");
        if (endpoint != null && !endpoint.isBlank()) {
            LOG.info("Using custom S3 endpoint: {}", endpoint);
            try {
                builder.endpointOverride(new URI(endpoint));
            }
            catch (URISyntaxException e) {
                throw new IllegalArgumentException("Invalid S3 endpoint: " + endpoint, e);
            }
        }

        String accessKey = env.get("AWS_ACCESS_KEY_ID");
        String secretKey = env.get("AWS_SECRET_ACCESS_KEY");
        AwsCredentialsProvider credentialsProvider;
        if (accessKey != null && secretKey != null) {
            LOG.info("Using explicit AWS credentials");
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey));
        }
        else {
            LOG.info("Using default AWS credentials provider chain");
            credentialsProvider = DefaultCredentialsProvider.create();
        }
        builder.credentialsProvider(credentialsProvider);

        builder.serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());

        S3Client client = builder.build();
        LOG.info("Initialized S3 client for region {}", client.serviceClientConfiguration().region());
        return client;
    }

    @Override
    public void close() {
        s3Client.close();
    }

    private record S3Location(String bucket, String key) {}
}
