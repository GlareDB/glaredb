---
title: Parquet
---

# Parquet Extension

The `parquet` extension enables direct querying of Parquet files. It is included
by default in the CLI, Python, and WebAssembly (Wasm) bindings.

## Functions

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
| uncompressed_sized | Uncompressed size of the row group in bytes. |
