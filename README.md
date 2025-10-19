# Trino s3file Plugin

A lightweight Trino connector that treats S3-compatible storage as a "schema on read" lake. It lets you query raw CSV and text files without provisioning an external catalog: metadata (column names, types, splits) are inferred at runtime directly from each object. Two table functions are exposed for ad-hoc exploration, data validation, or lightweight ingestion pipelines.

## Build and Run

```bash
docker compose up --build
```

## Connect with CLI

Use the Trino CLI bundled in the container:

```bash
docker compose exec -it trino trino --server http://localhost:8080
```

Once connected you can run the examples below.

## Seed Sample Files

Populate MinIO with demo data after the containers are up:

```bash
# create a bucket for the demo data
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket

# upload sample CSV (used by csv.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/example.csv s3://mybucket/data.csv

# upload sample text file (used by txt.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/messages.txt s3://mybucket/messages.txt
```

## csv.load Table Function

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

## txt.load Table Function

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
