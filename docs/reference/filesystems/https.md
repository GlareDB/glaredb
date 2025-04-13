---
title: HTTP(S)
---

# HTTP(S) File System

The HTTP(S) file system allows GlareDB to read data files directly from publicly
accessible URLs over HTTP or HTTPS. This is useful for quick access to hosted
datasets, public repositories, or files stored on static hosting services.

The HTTP(S) file system is enabled by default for the CLI, Python bindings, and
WebAssembly bindings.

## Usage

You can use any supported file-reading function with a fully qualified HTTP or
HTTPS URL.

```sql
SELECT * FROM read_csv('https://example.com/cities.csv');
```

This reads a remote CSV file over HTTPS and returns the parsed table.

You can also query HTTP(S) files directly by specifying the URL in the FROM clause:

```sql
SELECT * FROM 'https://example.com/cities.csv';
```

## Supported Formats

All supported file formats, such as CSV and Parquet, can be read using the
http(s) file system using their respective functions (e.g. `read_csv`).

## Notes

- The URL must be accessible without authentication. Private or restricted links
  are not supported at this time.
- Files are streamed or downloaded as needed, there’s no persistent caching.
- Redirections (3xx responses) are followed automatically.
- Reading large files over HTTP(S) may be slower than local access. Performance
  depends on network conditions and server response time.

> GlareDB does not currently support custom headers or authentication tokens
> (e.g. for APIs or signed URLs). Support for authenticated requests may be
> added in the future.
