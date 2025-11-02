# Trino s3file Plugin

A Trino connector for ad-hoc exploration, validation, or lightweight ingestion of JSON/CSV/TXT/XML S3 object files :

- **Schema inference on read**: metadata are inferred at runtime from each object.
- **Parameterized table functions**: pass parsing tweaks per query without redeploying.
- **Distributed processing**: workers stream byte ranges concurrently so oversized files stay readable.
- **Snapshot safety**: object versions or ETags are pinned to avoid mixing data when objects change mid-scan.

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

**Example input** (`docker/examples/events.jsonl`)

```json
{"event_id":1,"user":"alice","amount":19.5}
{"event_id":2,"user":"bob","amount":7.0,"metadata":{"source":"app"}}
{"event_id":3,"user":"carol"}
```

**Query output**

| event_id | user  | amount | metadata            |
|----------|-------|--------|---------------------|
| 1        | alice | 19.5   | NULL                |
| 2        | bob   | 7.0    | {"source":"app"}    |
| 3        | carol | NULL   | NULL                |

## Load XML files

```sql
SELECT *
FROM TABLE(
    s3file.xml.load(
        path => 's3://mybucket/books.xml',
        row_element => 'book',    -- element treated as one row
        include_text => 'false',  -- set to 'true' to expose mixed-content text
        empty_as_null => 'true',  -- convert empty strings to NULL when set to 'true'
        invalid_row_column => '_errors', -- capture unparsable rows as raw XML
        encoding => 'UTF-8'
    )
);
```

- `path` (required): XML document location in S3/MinIO.
- `row_element` (optional, default `'row'`): element name that represents one logical row; only direct children of that element become columns.
- `include_text` (optional, default `'false'`): expose mixed-content text (outside child elements) as an extra column named `text`.
- `empty_as_null` (optional, default `'false'`): convert empty attribute/element values to `NULL`.
- `invalid_row_column` (optional, default `'_errors'`): adds a `VARCHAR` column that receives the raw XML whenever a row cannot be projected (unexpected nesting, unknown fields, etc.). Use an empty string to disable it.
- `encoding` (optional, default `'UTF-8'`): character set for decoding the file.

Attributes are automatically projected as columns prefixed with `@`, while first-level child elements become `VARCHAR` columns. Nested structures are not flattened; compute additional parsing in SQL as needed.

`include_text` is useful for XML elements that interleave literal text with child nodes. For example, the snippet `<entry status="new">Reminder<message>Hello</message></entry>` yields columns `@status = 'new'`, `message = 'Hello'`, and, when `include_text => 'true'`, `text = 'Reminder'`.

Set `invalid_row_column` to keep malformed rows instead of failing the query. Those rows are emitted with `NULL` for projected fields and the raw XML in the dedicated column, which can then be inspected or reprocessed. Combine it with `empty_as_null => 'true'` to turn empty strings into SQL `NULL` while still capturing the original payload.

**Example input** (`docker/examples/books.xml`)

```xml
<catalog>
  <book id="bk101" tag="new">
    <author>Gambardella, Matthew</author>
    <title>XML Developer's Guide</title>
    <genre>Computer</genre>
    <price>44.95</price>
  </book>
  <book id="bk102" tag="old">
    <author>Ralls, Kim</author>
    <title>Midnight Rain</title>
    <genre>Fantasy</genre>
    <price>5.95</price>
  </book>
  <book id="bk103" tag="new">
    <author>Corets, Eva</author>
    <title>Maeve Ascendant</title>
    <genre/>
    <price>9.99</price>
  </book>
  <book id="bk104" tag="new">
    <title>
      <main>Advanced XML</main>
      <subtitle>Processing</subtitle>
    </title>
  </book>
</catalog>
```

**Query output**

| @id   | @tag | author              | title                  | genre    | price | _errors |
|-------|------|---------------------|------------------------|----------|-------|---------|
| bk101 | new  | Gambardella, Matthew| XML Developer's Guide  | Computer | 44.95 | NULL    |
| bk102 | old  | Ralls, Kim          | Midnight Rain          | Fantasy  | 5.95  | NULL    |
| bk103 | new  | Corets, Eva         | Maeve Ascendant        | NULL     | 9.99  | NULL    |

Rows containing nested structures that cannot be projected (for example `book id="bk104"` above) are emitted with `NULL` column values and the raw XML stored in `_errors`.

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

**Example input** (`docker/examples/example.csv`)

```csv
id;name;active
1;"Alice";true
2;"Bob";false
3;"Charlie";true
```

**Query output**

| id | name    | active |
|----|---------|--------|
| 1  | Alice   | true   |
| 2  | Bob     | false  |
| 3  | Charlie | true   |

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

**Example input** (`docker/examples/messages.txt`)

```text
Hello world
Another line
```

**Query output**

| line          |
|---------------|
| Hello world   |
| Another line  |

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
