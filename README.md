# Trino s3file Plugin

A Trino connector for ad-hoc exploration, validation, or lightweight ingestion of JSON/CSV/TXT S3 object files :

- **Schema inference on read**: metadata are inferred at runtime from each object.
- **Parameterized table functions**: pass parsing tweaks per query without redeploying.
- **Distributed processing**: workers stream byte ranges concurrently so oversized files stay readable.

## Load JSON files

```sql
SELECT *
FROM TABLE(
    s3file.json.load(
        path => 's3://mybucket/events.jsonl',
        encoding => 'UTF-8'      -- override when the file is not UTF-8
    )
);
```

- `path` (required): location of a newline-delimited JSON (NDJSON) object stream.
- `encoding` (optional, default `'UTF-8'`): override the charset used when decoding the object.
- `additional_columns` (optional): comma-separated list of `name:type` pairs for fields that might not appear in the first JSON object. Types currently supported: `boolean`, `bigint`, `double`, `varchar`. Duplicate names override the inferred type.

Fields and types are inferred from the first JSON object: booleans map to `boolean`, integral numbers to `bigint`, floating numbers to `double`, nested objects/arrays stay as JSON text (`varchar`), and other values remain `varchar`.

## Load XML files

```sql
SELECT *
FROM TABLE(
    s3file.xml.load(
        path => 's3://mybucket/books.xml',
        row_element => 'book',    -- element treated as one row
        include_text => 'false',  -- set to 'true' to expose mixed-content text
        empty_as_null => 'true',  -- convert empty strings to NULL when set to 'true'
        encoding => 'UTF-8'
    )
);
```

- `path` (required): XML document location in S3/MinIO.
- `row_element` (optional, default `'row'`): element name that represents one logical row; only direct children of that element become columns.
- `include_text` (optional, default `'false'`): expose mixed-content text (outside child elements) as an extra column named `text`.
- `empty_as_null` (optional, default `'false'`): convert empty attribute/element values to `NULL`.
- `encoding` (optional, default `'UTF-8'`): character set for decoding the file.

Attributes are automatically projected as columns prefixed with `@`, while first-level child elements become `VARCHAR` columns. Nested structures are not flattened; compute additional parsing in SQL as needed.

## Load CSV files

```sql
SELECT *
FROM TABLE(
    s3file.csv.load(
        path => 's3://mybucket/data.csv',
        delimiter => ';',
        header => 'true',        -- set to 'false' when the CSV has no header row
        encoding => 'UTF-8'      -- override when the file is not UTF-8
    )
);
```

- `path` (required): CSV location in S3/MinIO.
- `delimiter` (optional, default `';'`): single character separator.
- `header` (optional, default `'true'`): when `'false'`, the first row is treated as data and column names default to `column_1`, `column_2`, …
- `encoding` (optional, default `'UTF-8'`): character set for decoding the file.

The function returns all values as `VARCHAR`; cast in SQL as needed.

## Load TXT files

```sql
SELECT *
FROM TABLE(
    s3file.txt.load(
        path => 's3://mybucket/messages.txt',
        line_break => '\n',  -- override with '\r\n' or any custom separator
        encoding => 'UTF-8'  -- override when the file is not UTF-8
    )
);
```

- `path` (required): text file location in S3/MinIO.
- `line_break` (optional, default `'\n'`): string separator used to split the file into rows.
- `encoding` (optional, default `'UTF-8'`): character set for decoding the file.

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

# upload a simple XML document (used by xml.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/books.xml s3://mybucket/books.xml
```
