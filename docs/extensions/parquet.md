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

### Direct URI Querying

Parquet files can also be queried directly by using the file path or URI in the FROM clause:

```sql
SELECT * FROM 'cities.parquet';
```

### `parquet_file_metadata`

Returns high-level metadata about a Parquet file.

| Column         | Description                                     |
|----------------|-------------------------------------------------|
| filename       | Name of the file being queried.                 |
| version        | Parquet format version used in the file.        |
| num_rows       | Total number of rows in the file.               |
| create_by      | Application or library that wrote the file.     |
| num_row_groups | Number of row groups contained within the file. |

### `parquet_rowgroup_metadata`

Returns metadata for each row group within a Parquet file.

| Column            | Description                                  |
|-------------------|----------------------------------------------|
| filename          | Name of the file being queried.              |
| num_rows          | Number of rows in the row group.             |
| num_columns       | Number of columns in the row group.          |
| uncompressed_size | Uncompressed size of the row group in bytes. |
| ordinal           | Zero-based ordinal of the row group within the file.         |

### `parquet_column_metadata`

Returns metadata for each column in each row group within a Parquet file.

| Column                  | Description                                        |
|-------------------------|----------------------------------------------------|
| filename                | Name of the file being queried.                    |
| rowgroup_ordinal        | Zero-based ordinal of the row group within the file.               |
| column_ordinal          | Zero-based ordinal of the column within the row group. |
| physical_type           | Physical storage type of the column (e.g., INT32, BYTE_ARRAY). |
| max_definition_level    | Maximum definition level for the column.           |
| max_repetition_level    | Maximum repetition level for the column.           |
| file_offset             | Byte offset of the column chunk in the file.       |
| num_values              | Number of values stored in the column chunk.       |
| total_compressed_size   | Compressed size of the column chunk in bytes.      |
| total_uncompressed_size | Uncompressed size of the column chunk in bytes.    |
| data_page_offset        | Byte offset from beginning of file to first data page. |
