---
title: CSV
---

# CSV extension

The `csv` extension enables reading of CSV files.

This extension is included by default in the CLI, Python, and WebAssembly
clients for GlareDB. A `csv` schema will be created automatically containing all
CSV related functions.

## Reading CSV files

The `read_csv` table function can be used to read a CSV file:

```sql
SELECT * FROM read_csv('path/to/file.csv');
```

The `read_csv` file can also be used to read TSV (Tab-Separated Values) files as
well:

```sql
SELECT * FROM read_csv('path/to/file.tsv');
```

When reading a CSV or TSV file, the "dialect" used will be inferred. This
includes inferring the delimeter (comma for CSV, tab for TSV) as well as if the
file contains a header. Once the dialect has been determined, the types for each
record will then be inferred.

[DESCRIBE](../reference/sql/commands/describe.md) can be used to determine the
types that were inferred for each column:

```sql
DESCRIBE read_csv('path/to/file.csv');
```

`read_csv` is an alias for the namespaced `csv.read` function, and can be used
interchangeably:

```sql
SELECT * FROM csv.read('path/to/file.csv');
```

If your CSV files ends with `.csv` or `.tsv`, the function call can be omitted.
The function to use will be inferred automatically:

```sql
SELECT * FROM 'path/to/file.csv';
```

Multiple files can be provided using either a list of files, or a glob. All
files currently expected to have the same schema.

To read a specific set of files:

```sql
SELECT * FROM read_csv(['file1.csv', 'file2.csv']);
```

To read all CSV files in `data/`:

```sql
SELECT * FROM read_csv('data/*.csv');
```

If the glob ends with `.csv` or `.tsv`, the function call be omitted:

```sql
SELECT * FROM 'data/*.csv';
```

## Function reference

All CSV table functions are located in the `csv` schema. A complete list can be
found using `list_functions`:

```sql
SELECT *
FROM list_functions()
WHERE schema_name = 'csv';
```

### `csv.read`

**Alias**: `read_csv`, `csv_scan`, `csv.scan`.

The `read_csv` function takes a path to a CSV file and returns a table
containing the parsed data.

```sql
SELECT * FROM read_csv('userdata1.csv');
```

By default, `read_csv` will:

- Automatically infer column data types
- Detect and skip the header row (if present)

Additional paramters can be provided for other file systems. For example, we can
provide AWS credentials for accessing a CSV file in a private S3 bucket:

```sql
SELECT * FROM csv.read('s3://bucket-name/path/to/file.csv',
                       region='us-east-1',
                       access_key_id='YOUR_ACCESS_KEY',
                       secret_access_key='YOUR_SECRET_KEY');
```

> Note: Support for custom parsing options, including explicit type definitions
> and header row overrides, will be available soon.
