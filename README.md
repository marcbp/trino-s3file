# Trino s3file Plugin

[![Tests](https://github.com/marcbp/trino-s3file/actions/workflows/tests.yml/badge.svg)](https://github.com/marcbp/trino-s3file/actions/workflows/tests.yml)

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
        path => 's3://mybucket/data.jsonl',
        additional_columns => 'nickname:varchar' -- include fields missing from the first object
    )
);
```

- `path` (required): location of a newline-delimited JSON (NDJSON) object stream.
- `encoding` (optional, default `'UTF-8'`): override the charset used when decoding the object.
- `split_size_mb` (optional, default connector value `32`): target split size in MiB for distributed reads.
- `additional_columns` (optional): comma-separated list of `name:type` pairs for fields that might not appear in the first JSON object. Types currently supported: `boolean`, `bigint`, `double`, `varchar`. Duplicate names override the inferred type.

Fields and types are inferred from the first JSON object: booleans map to `boolean`, integral numbers to `bigint`, floating numbers to `double`, nested objects/arrays stay as JSON text (`varchar`), and other values remain `varchar`.

**Example input** (`docker/examples/data.jsonl`)

```json
{"id":1,"firstname":"André","lastname":"Merlaux","age":25,"status":"active"}
{"id":2,"firstname":"Roger","lastname":"Moulinier","age":46,"status":"active"}
{"id":3,"firstname":"Jacky","lastname":"Jacquard","age":44,"status":"active"}
{"id":4,"firstname":"Jean-René","lastname":"Calot","age":47,"status":"active"}
{"id":5,"firstname":"Georges","lastname":"Préjean","nickname":"Moïse","age":67,"status":"inactive"}
```

**Query output**

| id | firstname  | lastname  | nickname | age | status   |
|----|------------|-----------|----------|-----|----------|
| 1  | André      | Merlaux   | null     | 25  | active   |
| 2  | Roger      | Moulinier | null     | 46  | active   |
| 3  | Jacky      | Jacquard  | null     | 44  | active   |
| 4  | Jean-René  | Calot     | null     | 47  | active   |
| 5  | Georges    | Préjean   | Moïse    | 67  | inactive |

## Load XML files

```sql
SELECT *
FROM TABLE(
    s3file.xml.load(
        path => 's3://mybucket/data.xml',
        row_element => 'employee', -- element treated as one row
        empty_as_null => 'true',  -- convert empty strings to NULL when set to 'true'
        invalid_row_column => '', -- disable the default raw-error column for this example
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

**Example input** (`docker/examples/data.xml`)

```xml
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
```

**Query output**

| @id | firstname  | lastname  | nickname | age | status   |
|-----|------------|-----------|----------|-----|----------|
| 1   | André      | Merlaux   | NULL     | 25  | active   |
| 2   | Roger      | Moulinier | NULL     | 46  | active   |
| 3   | Jacky      | Jacquard  | NULL     | 44  | active   |
| 4   | Jean-René  | Calot     | NULL     | 47  | active   |
| 5   | Georges    | Préjean   | Moïse    | 67  | inactive |

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
- `split_size_mb` (optional, default connector value `32`): target split size in MiB for distributed reads.

The function returns all values as `VARCHAR`; cast in SQL as needed.

**Example input** (`docker/examples/data.csv`)

```csv
id;firstname;lastname;nickname;age;status
1;André;Merlaux;;25;active
2;Roger;Moulinier;;46;active
3;Jacky;Jacquard;;44;active
4;Jean-René;Calot;;47;active
5;Georges;Préjean;Moïse;67;inactive
```

**Query output**

| id | firstname  | lastname  | nickname | age | status   |
|----|------------|-----------|----------|-----|----------|
| 1  | André      | Merlaux   |          | 25  | active   |
| 2  | Roger      | Moulinier |          | 46  | active   |
| 3  | Jacky      | Jacquard  |          | 44  | active   |
| 4  | Jean-René  | Calot     |          | 47  | active   |
| 5  | Georges    | Préjean   | Moïse    | 67  | inactive |

## Load TXT files

```sql
SELECT *
FROM TABLE(
    s3file.txt.load(
        path => 's3://mybucket/data.txt',
        line_break => '\n',  -- override with '\r\n' or any custom separator
        encoding => 'UTF-8'  -- override when the file is not UTF-8
    )
);
```

- `path` (required): text file location in S3/MinIO.
- `line_break` (optional, default `'\n'`): string separator used to split the file into rows.
- `encoding` (optional, default `'UTF-8'`): character set for decoding the file.
- `split_size_mb` (optional, default connector value `32`): target split size in MiB for distributed reads.

The function yields a single `VARCHAR` column named `line` containing each record in order.

**Example input** (`docker/examples/data.txt`)

```text
id=1 firstname=André lastname=Merlaux age=25 status=active
id=2 firstname=Roger lastname=Moulinier age=46 status=active
id=3 firstname=Jacky lastname=Jacquard age=44 status=active
id=4 firstname=Jean-René lastname=Calot age=47 status=active
id=5 firstname=Georges lastname=Préjean nickname=Moïse age=67 status=inactive
```

**Query output**

| line                                                                  |
|-----------------------------------------------------------------------|
| id=1 firstname=André lastname=Merlaux age=25 status=active            |
| id=2 firstname=Roger lastname=Moulinier age=46 status=active          |
| id=3 firstname=Jacky lastname=Jacquard age=44 status=active           |
| id=4 firstname=Jean-René lastname=Calot age=47 status=active          |
| id=5 firstname=Georges lastname=Préjean nickname=Moïse age=67 status=inactive |

## Parallelism Limits

The connector can process `txt`, `csv`, and `json` files in parallel by splitting the object into byte ranges. This works best when records are much smaller than the configured split size.

If a logical record is very large, a worker can fail the parse or return an incomplete row when the record crosses a split boundary. The simplest workaround is to increase `split_size_mb` (default to 32MB) so the largest expected record fits comfortably inside one split.

## Quickstart

### Connector Configuration

Set the connector-wide split size default in the catalog properties:

```properties
connector.name=s3file
s3.default-split-size-mb=32
```

`split_size_mb` on `txt.load`, `csv.load`, and `json.load` overrides this value per query. `xml.load` still reads whole files.

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
docker compose exec -it trino-coordinator trino --server http://localhost:8080
```

Once connected you can run the examples below.

### Seed Sample Files

Populate MinIO with demo data after the containers are up:

```bash
# MinIO credentials from docker-compose.yml
export AWS_ACCESS_KEY_ID=minio
export AWS_SECRET_ACCESS_KEY=minio123
export AWS_DEFAULT_REGION=us-east-1

# create a bucket for the demo data
aws --endpoint-url http://localhost:9000 s3 mb s3://mybucket

# upload sample CSV (used by csv.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/data.csv s3://mybucket/data.csv

# upload sample JSON stream (used by json.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/data.jsonl s3://mybucket/data.jsonl

# upload sample text file (used by txt.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/data.txt s3://mybucket/data.txt

# upload a simple XML document (used by xml.load example)
aws --endpoint-url http://localhost:9000 s3 cp docker/examples/data.xml s3://mybucket/data.xml
```

## License

Distributed under the Apache License 2.0.
