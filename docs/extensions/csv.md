---
title: CSV
---

# CSV Extension

The `csv` extension enables direct querying of CSV files. It is included by
default in the CLI, Python, and WebAssembly (Wasm) bindings.

## Functions

### `read_csv`

**Alias**: `scan_csv`

The `read_csv` function takes a path to a CSV file and returns a table
containing the parsed data.

```sql
SELECT * FROM read_csv('../testdata/csv/userdata1.csv');
```

By default, `read_csv` will:

- Automatically infer column data types
- Detect and skip the header row (if present)

You can inspect the inferred column names and types using the DESCRIBE
statement:

```
DESCRIBE read_csv('../testdata/csv/userdata1.csv');
```

This returns a table with the name and data type of each column:

| column_name       | datatype |
|-------------------|----------|
| registration_dttm | Utf8     |
| id                | Int64    |
| first_name        | Utf8     |
| last_name         | Utf8     |
| email             | Utf8     |
| gender            | Utf8     |
| ip_address        | Utf8     |
| cc                | Utf8     |
| country           | Utf8     |
| birthdate         | Utf8     |
| salary            | Utf8     |
| title             | Utf8     |
| comments          | Utf8     |

> Note: Support for custom parsing options, including explicit type definitions
> and header row overrides, will be available soon.
