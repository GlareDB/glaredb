# `csv_scan`

Read csv files from a local filesystem or supported object stores.

The files may be compressed, for example: `.csv.gz`.

URLs provided to `csv_scan` may also include glob characters to allow
scanning multiple files when reading from S3, GCS, or a local filesystem.
Globbed paths are not allowed when querying csv files over HTTP/HTTPS.

## Syntax

```sql
-- Single url or path.
csv_scan(<url>)
-- Multiple urls or paths.
csv_scan([<url>])
-- Using a cloud credentials object.
csv_scan(<url>, <credential_object>)
-- Required named argument for S3 buckets.
csv_scan(<url>, <credentials_object>, region => '<aws_region>')
-- Pass S3 credentials using named arguments.
csv_scan(<url>, access_key_id => '<aws_access_key_id>', secret_access_key => '<aws_secret_access_key>', region => '<aws_region>')
-- Pass GCS credentials using named arguments.
csv_scan(<url>, service_account_key => '<gcp_service_account_key>')
```

Parameter descriptions and which object store it's relevant to.

| Parameter                 | Object store | Description                                                                |
| ------------------------- | ------------ | -------------------------------------------------------------------------- |
| `url`                     | All          | The URL or path to a csv file to scan.                                     |
| `credential_object`       | S3 and GCS   | A database object storing credentials for accessing the object or objects. |
| `aws_region`              | S3           | If scanning an object in S3, the region of the bucket.                     |
| `aws_access_key_id`       | S3           | ID of AWS access key with permissions to read from the bucket.             |
| `aws_secret_access_key`   | S3           | Secret associated with the AWS access key.                                 |
| `gcp_service_account_key` | GCS          | A JSON-encoded GCP service account key with access to the bucket.          |

## Usage

### Local files

Local files can be read by passing the path or a globbed path to `csv_scan`.
Paths may be absolute or relative.

```sql
-- Read a relative path.
SELECT * FROM csv_scan('./my_data.csv');
-- Read all csv files in a directory.
SELECT * FROM csv_scan('./directory_of_data/*.csv');
-- Read csv files from multiple directories.
SELECT * FROM csv_scan('./**/*.csv');
-- Read multiple explicitly provided files.
SELECT * FROM csv_scan(['./directory_of_data/1.csv', './directory_of_data/2.csv']);
```

### Objects in GCS

Objects in GCS can be read by providing the path to the object and credentials
for the object to `csv_scan`. The service account key provided should be
JSON encoded.

```sql
-- Single object.
SELECT * FROM csv_scan('gs://my-bucket/path/object.csv',
                           service_account_key => '<service_account_key>');
-- Scan objects matching a glob.
SELECT * FROM csv_scan('gs://my-bucket/path/prefix_*.csv',
                           service_account_key => '<service_account_key>');
-- Read all objects in a directory.
SELECT * FROM csv_scan('gs://my-bucket/path/*',
                           service_account_key => '<service_account_key>');
```

Optionally, a credentials object can be created and used in place of providing
the service account key directly.

```sql
-- Create the credentials.
CREATE CREDENTIALS my_gcp_creds PROVIDER gcp
    ( service_account_key '<service_account_key>' );
-- And use them in the scan.
SELECT * FROM csv_scan('gs://my-bucket/path/object.csv', my_gcp_creds);
```

### Objects in S3

Objects in GCS can be read by providing the path to the object and credentials
for the object, and the bucket region to `csv_scan`.

```sql
SELECT * FROM csv_scan('gs://my-bucket/path/object.csv',
                           access_key_id = '<access_key_id>',
                           secret_access_key = '<secret_access_key>',
                           region = 'us-east-1');
-- Scan objects matching a glob.
SELECT * FROM csv_scan('gs://my-bucket/path/prefix_*.csv',
                           access_key_id = '<access_key_id>',
                           secret_access_key = '<secret_access_key>',
                           region = 'us-east-1');
-- Read all objects in a directory.
SELECT * FROM csv_scan('gs://my-bucket/path/prefix_*.csv',
                           access_key_id = '<access_key_id>',
                           secret_access_key = '<secret_access_key>',
                           region = 'us-east-1');
```

Optionally, a credentials object can be created and used in place of providing
the service account key directly.

```sql
-- Create the credentials.
CREATE CREDENTIALS my_aws_creds PROVIDER aws
    OPTIONS (
        access_key_id = '<access_key_id>',
        secret_access_key = '<secret_access_key>',
    );
-- And use them in the scan. Note that a region still needs to be provided.
SELECT * FROM csv_scan('gs://my-bucket/path/object.csv', my_aws_creds, region => 'us-east-1');
```

### HTTP files

CSV files can also be scanned over HTTP/HTTPS.

```sql
-- Read a single file.
SELECT * FROM csv_scan('https://my_domain.com/file.csv');
-- Read multiple files
SELECT * FROM csv_scan(['https://my_domain.com/1.csv', 'https://my_domain.com/2.csv']);
```

> Globbed paths are not supported when reading csv files over HTTP/HTTPS

## Examples

In the following example we use [COPY TO] to create a csv file and finally
read it with `csv_scan`.

```sql
-- Output the file to 'example.csv'
COPY (SELECT * FROM generate_series(1, 10)) TO 'example.csv';

-- And scan it back in.
SELECT * FROM csv_scan('./example.csv');

-- Output:
-- ┌─────────────────┐
-- │ generate_series │
-- │ ──              │
-- │ Int64           │
-- ╞═════════════════╡
-- │ 1               │
-- │ 2               │
-- │ 3               │
-- │ 4               │
-- │ 5               │
-- │ 6               │
-- │ 7               │
-- │ 8               │
-- │ 9               │
-- │ 10              │
-- └─────────────────┘
```
