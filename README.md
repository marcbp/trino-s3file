# Trino s3file Plugin

A lightweight Trino connector exposing a single table function `csv.load` that reads CSV files from S3-compatible storage.

## Build and Run

```bash
docker compose up --build
```

## Table Function Usage

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

## MinIO Test Flow

```bash
# create bucket and upload sample CSV
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket
aws --endpoint-url http://localhost:9000 s3 cp docker/csv/example.csv s3://mybucket/data.csv
```

Then connect to Trino (`./scripts/trino-cli.sh`) and execute the query above.
