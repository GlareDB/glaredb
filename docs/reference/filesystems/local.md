---
title: Local
---

# Local File System

The local file system is the simplest and most straightforward way for GlareDB
to access data—it reads files directly from the machine’s file system, without
any additional setup or configuration.

The local file system is only available when using the CLI or Python bindings,
as the WebAssembly bindings do not have access to the local file system.

## Usage

You can query files using either relative or absolute paths.

### Reading with a relative path

```sql
SELECT * FROM read_csv('./cities.csv');
```

This reads the `cities.csv` file relative to the current working directory where
GlareDB is running.

### Reading with an absolute path

```sql
SELECT * FROM read_csv('/home/user/data/cities.csv');
```

This reads the file directly from the specified location in the file system.

## Supported Formats

All supported file formats, such as CSV and Parquet, can be read from the local
file system using their respective functions (e.g. `read_csv`).




