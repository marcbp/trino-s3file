# Trino s3file Plugin

A lightweight Trino connector that treats S3-compatible storage as a "schema on read" lake. It lets you query raw CSV and text files without provisioning an external catalog: metadata (column names, types, splits) are inferred at runtime directly from each object. Two table functions are exposed for ad-hoc exploration, data validation, or lightweight ingestion pipelines.

## Build and Run

```bash
docker compose up --build
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
