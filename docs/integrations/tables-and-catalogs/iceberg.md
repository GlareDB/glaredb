---
title: Iceberg
---

# Iceberg extension

The `iceberg` extension enables interacting with [Apache
Iceberg](https://iceberg.apache.org/) tables and catalogs.

This extension is included by default in the CLI, Python, and WebAssembly
clients for GlareDB. An `iceberg` schema will be created automatically
containing all Iceberg related functions.

> The `iceberg` extension is under heavy development, and functionality may be
> missing or incomplete.

## Function reference

All Iceberg table functions accept a table path as the first argument. The table
path should be a directory containing `metadata/` and `data/` sub-directories.

An additional `version` parameter may be passed to all table functions
specifying the version of the table to read. This version is used to determine
which metadata JSON to load.

For example, if we wanted to query metadata of version "00004" of an Iceberg
table, we can specify the `version` in the function call:

```sql
SELECT *
FROM iceberg.metadata('path/to/table', version = '00004')
```

This will attempt to load the metadata for version "00004". Note that metadata
JSON files typically have a UUID in the file name as well -- the UUID does not
need to be provided in the version string.

If `version` is not provided, the latest version will be read by attempting to
list all metadata JSON files in the `metadata/` directory and find the file with
the lexicographically greatest name.

### `iceberg.metadata`

The `iceberg.metadata` function takes a table path and returns information from
the table's metadata.

```sql
SELECT *
FROM iceberg.metadata('wh/default.db/cities')
```

| Column           | Description                              |
|------------------|------------------------------------------|
| `format_version` | The Iceberg format version of the table. |
| `table_uuid`     | The UUID identifier for the table.       |
| `location`       | Base location of the table.              |

### `iceberg.snapshots`

The `iceberg.snapshots` function takes a table path and returns information
about valid snapshots for this version of the table.

```sql
SELECT *
FROM iceberg.snapshots('wh/default.db/cities')
```

| Column            | Description                                           |
|-------------------|-------------------------------------------------------|
| `snapshot_id`     | A unique ID for the snapshot.                         |
| `sequence_number` | Number indicating the order of changes to this table. |
| `manifest_list`   | Location of the manifest list for this table.         |

### `iceberg.manifest_list`

The `iceberg.manifest_list` function takes a table path and returns information
about the manifest list for the current snapshot.

```sql
SELECT *
FROM iceberg.manifest_list('wh/default.db/cities')
```

| Column            | Description                                             |
|-------------------|---------------------------------------------------------|
| `manifest_path`   | Location of a manifest file.                            |
| `manifest_length` | Length in bytes of the manifest file.                   |
| `content`         | Type of files the manifest is for, 'data' or 'deletes'. |
| `sequence_number` | Sequence number when manifest was added.                |

### `iceberg.data_files`

The `iceberg.data_files` function takes a table path and returns information
about the data files for the current snapshot.

```sql
SELECT *
FROM iceberg.data_files('wh/default.db/cities')
```

| Column         | Description                                                                         |
|----------------|-------------------------------------------------------------------------------------|
| `status`       | 'EXISTING', 'ADDED', or 'DELETED'                                                   |
| `content`      | Type of content in the data file, 'DATA', 'POSITION DELETES', or 'EQUALITY DELETES' |
| `file_path`    | Full URI for the file.                                                              |
| `file_format`  | Format of the file.                                                                 |
| `record_count` | Number of records in the file.                                                      |
