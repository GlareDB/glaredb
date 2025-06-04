---
title: GCS
---

# Google Cloud Storage (GCS) file system

The Google Cloud Storage (GCS) file system allows GlareDB to read data directly from Google Cloud Storage buckets.
This makes it easy to query data stored in cloud-based data lakes without
needing to download files manually.

The GCS file system is enabled by default for the CLI, Python bindings, and
WebAssembly bindings.

## Usage

To read from a GCS bucket, use any supported file-reading function with a
`gs://` URI:

```sql
SELECT * FROM read_parquet('gs://my-bucket/data.parquet');
```

This reads the `data.parquet` file from the `my-bucket` bucket.

You can also query GCS files directly by specifying the GCS URI in the FROM clause:

```sql
SELECT * FROM 'gs://my-bucket/data.parquet';
```

## Optional parameters

When accessing private GCS buckets, you can provide a service account key directly as
an option:

```sql
SELECT * FROM read_parquet(
  'gs://my-private-bucket/secure.parquet',
  service_account = '{SERVICE_ACCOUNT_JSON}'
);
```

### Supported Options

| Option            | Description                                                                 |
|-------------------|-----------------------------------------------------------------------------|
| `service_account` | GCP Service Account key in JSON format                                      |

## Supported formats

All supported file formats, such as CSV and Parquet, can be read using the GCS
file system using their respective functions (e.g. `read_parquet`).

## CORS configuration for WebAssembly

When accessing GCS buckets from GlareDB running in a browser using WebAssembly,
you need to configure Cross-Origin Resource Sharing (CORS) for your GCS bucket to
allow browser-based requests.

To configure CORS for your GCS bucket:

1. Create a JSON file (e.g., `cors.json`) with the following configuration:

```json
[
    {
        "origin": ["*"],
        "method": ["GET", "HEAD"],
        "responseHeader": ["*"],
        "maxAgeSeconds": 3600
    }
]
```

2. Apply the configuration using the `gsutil` command-line tool:

```
gsutil cors set cors.json gs://<my-bucket>
```

This configuration allows GET and HEAD requests from any origin, which is
required for GlareDB's WebAssembly bindings to access your GCS bucket.

For more information about CORS configuration for GCS, see the [Google Cloud
documentation](https://cloud.google.com/storage/docs/using-cors).

## Notes

- Public files can be accessed without authentication.
- A service account key must be provided when querying non-public buckets.
