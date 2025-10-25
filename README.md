# Trino s3file Plugin

A Trino connector for ad-hoc exploration, validation, or lightweight ingestion of S3 object files :

- **Schema inference on read**: metadata are inferred at runtime from each object.
- **Parameterized table functions**: pass delimiters, headers, and other parsing tweaks per query without redeploying.
- **Distributed processing of large inputs**: byte-range splits stream big CSV/TXT/JSON files in parallel across workers.

## Load JSON files

```sql
SELECT *
FROM TABLE(
    s3file.json.load(
        path => 's3://mybucket/events.jsonl'
    )
);
```

- `path` (required): location of a newline-delimited JSON (NDJSON) object stream.

Fields are inferred from the first JSON object and emitted as `VARCHAR` columns. Values keep their textual form for easy casting in SQL.

## Load CSV files

```sql
SELECT *
FROM TABLE(
    s3file.csv.load(
        path => 's3://mybucket/data.csv',
        delimiter => ';',
        header => 'true'  -- set to 'false' when the CSV has no header row
    )
);
```

- `path` (required): CSV location in S3/MinIO.
- `delimiter` (optional, default `';'`): single character separator.
- `header` (optional, default `'true'`): when `'false'`, the first row is treated as data and column names default to `column_1`, `column_2`, …

The function returns all values as `VARCHAR`; cast in SQL as needed.

## Load TXT files

```sql
SELECT *
FROM TABLE(
    s3file.txt.load(
        path => 's3://mybucket/messages.txt',
        line_break => '\n'  -- override with '\r\n' or any custom separator
    )
);
```

- `path` (required): text file location in S3/MinIO.
- `line_break` (optional, default `'\n'`): string separator used to split the file into rows.

The function yields a single `VARCHAR` column named `line` containing each record in order.

## Quickstart

### Build and Run

```bash
docker compose up --build
```

### Run Tests 

```bash
docker compose run --rm tests mvn test
```

### Connect with CLI

Use the Trino CLI bundled in the container:

```bash
docker compose exec -it trino trino --server http://localhost:8080
```

Once connected you can run the examples below.

### Seed Sample Files

Populate MinIO with demo data after the containers are up:

```bash
# create a bucket for the demo data
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket

# upload sample CSV (used by csv.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/example.csv s3://mybucket/data.csv

# upload sample JSON stream (used by json.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/events.jsonl s3://mybucket/events.jsonl

# upload sample text file (used by txt.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/messages.txt s3://mybucket/messages.txt
```
