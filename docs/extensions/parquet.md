---
title: Parquet
---

# Parquet Extension

The `parquet` extension enables direct querying of Parquet files. It is included
by default in the CLI, Python, and WebAssembly (Wasm) bindings.

## Functions

### `read_parquet`

**Alias**: `parquet_scan`

The `read_parquet` function takes a path to a Parquet file and returns a table
containing the data.

```sql
SELECT * FROM read_parquet('cities.parquet');
```

By default, `read_parquet` will automatically infer column data types from the
Parquet file schema.

You can inspect the inferred column names and types using the DESCRIBE
statement:

```sql
DESCRIBE read_parquet('cities.parquet');
```

This returns a table with the name and data type of each column.

For S3 sources, additional parameters can be provided:

```sql
SELECT * FROM read_parquet('s3://bucket-name/path/to/file.parquet', 
                          region='us-east-1', 
                          access_key_id='YOUR_ACCESS_KEY', 
                          secret_access_key='YOUR_SECRET_KEY');
```

### `parquet_file_metadata`

Returns high-level metadata about a Parquet file.

| Column         | Description                                     |
|----------------|-------------------------------------------------|
| file_name      | Name of the file being queried.                 |
| version        | Parquet format version used in the file.        |
| num_rows       | Total number of rows in the file.               |
| create_by      | Application or library that wrote the file.     |
| num_row_groups | Number of row groups contained within the file. |

### `parquet_rowgroup_metadata`

Returns metadata for each row group within a Parquet file.

| Column             | Description                                  |
|--------------------|----------------------------------------------|
| file_name          | Name of the file being queried.              |
| num_rows           | Number of rows in the row group.             |
| num_columns        | Number of columns in the row group.          |
| uncompressed_size  | Uncompressed size of the row group in bytes. |
