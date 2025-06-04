---
title: Parquet
---

# Parquet extension

The `parquet` extension enables direct querying of [Apache
Parquet](https://parquet.apache.org/) files. Parquet is a columnar file format
for efficient analytical querying.

This extension is included by default in the CLI, Python, and WebAssembly
clients for GlareDB. A `parquet` schema will be created automatically containing
all Parquet related functions.

## Reading Parquet files

The `read_parquet` table function can be used to read a parquet file:

```sql
SELECT * FROM read_parquet('path/to/file.parquet');
```

`read_parquet` is an alias for the namespaced `parquet.read` function, and can
be used interchangeably:

```sql
SELECT * FROM parquet.read('path/to/file.parquet');
```

If your Parquet file ends with `.parquet`, the table function can be omitted
entirely. The function to use will be inferred automatically:

```sql
SELECT * FROM 'path/to/file.parquet';
```

Multiple files can be provided using either a list of files, or a glob. All
files currently expected to have the same schema.

To read a specific set of files:

```sql
SELECT * FROM read_parquet(['file1.parquet', 'file2.parquet']);
```

To read all parquet files in `data/`:

```sql
SELECT * FROM read_parquet('data/*.parquet');
```

If the glob ends with `.parquet`, the function call be omitted:

```sql
SELECT * FROM 'data/*.parquet';
```

## Function reference

All Parquet table functions are located in the `parquet` schema. A complete
list can be found using `list_functions`:

```sql
SELECT *
FROM list_functions()
WHERE schema_name = 'parquet';
```

### `parquet.read`

**Aliases**: `read_parquet`, `parquet_scan`, `parquet.scan`

The `parquet.read` function takes a path to a Parquet file and returns a table
containing the data.

```sql
SELECT * FROM parquet.read('cities.parquet');
```

Additional paramters can be provided for other file systems. For example, we can
provide AWS credentials for accessing a parquet file in a private S3 bucket:

```sql
SELECT * FROM parquet.read('s3://bucket-name/path/to/file.parquet',
                           region='us-east-1',
                           access_key_id='YOUR_ACCESS_KEY',
                           secret_access_key='YOUR_SECRET_KEY');
```

### `parquet_file_metadata`

Returns high-level metadata about a Parquet file.

```sql
SELECT * FROM parquet.file_metadata('cities.parquet');
```

| Column           | Description                                     |
|------------------|-------------------------------------------------|
| `filename`       | Name of the file being queried.                 |
| `version`        | Parquet format version used in the file.        |
| `num_rows`       | Total number of rows in the file.               |
| `created_by`     | Application or library that wrote the file.     |
| `num_row_groups` | Number of row groups contained within the file. |

### `parquet.rowgroup_metadata`

Returns metadata for each row group within a Parquet file.

```sql
SELECT * FROM parquet.rowgroup_metadata('cities.parquet');
```

| Column              | Description                                          |
|---------------------|------------------------------------------------------|
| `filename`          | Name of the file being queried.                      |
| `num_rows`          | Number of rows in the row group.                     |
| `num_columns`       | Number of columns in the row group.                  |
| `uncompressed_size` | Uncompressed size of the row group in bytes.         |
| `ordinal`           | Zero-based ordinal of the row group within the file. |

### `parquet.column_metadata`

Returns metadata for each column in each row group within a Parquet file.

```sql
SELECT * FROM parquet.column_metadata('cities.parquet');
```

| Column                    | Description                                                    |
|---------------------------|----------------------------------------------------------------|
| `filename`                | Name of the file being queried.                                |
| `rowgroup_ordinal`        | Zero-based ordinal of the row group within the file.           |
| `column_ordinal`          | Zero-based ordinal of the column within the row group.         |
| `physical_type`           | Physical storage type of the column (e.g., INT32, BYTE_ARRAY). |
| `max_definition_level`    | Maximum definition level for the column.                       |
| `max_repetition_level`    | Maximum repetition level for the column.                       |
| `file_offset`             | Byte offset of the column chunk in the file.                   |
| `num_values`              | Number of values stored in the column chunk.                   |
| `total_compressed_size`   | Compressed size of the column chunk in bytes.                  |
| `total_uncompressed_size` | Uncompressed size of the column chunk in bytes.                |
| `data_page_offset`        | Byte offset from beginning of file to first data page.         |
