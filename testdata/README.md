# Testdata

Data useful for testing.

## Iceberg

Generating iceberg data is done with the `generate_iceberg.py` script. This
script requires `pyspark` as well as a [Spark runtime jar](https://iceberg.apache.org/releases/).
The jar should be placed in this directory (jars are in gitignore, so don't
worry about accidentally checking it in).

Then just call the script with python:

```
$ python generate_iceberg.py
```

This will generate various iceberg tables in `./iceberg/tables` using source
data from parquet files in `./iceberg/source_data`.

Test data has also been uploaded to GCS and S3 with the following commands:

```
$ gsutil cp -r iceberg/ gs://glaredb-test/iceberg
$ aws s3 cp --recursive iceberg/ s3://glaredb-test/iceberg
```

### Source data

Source data was generated with sql queries like the following:

```sql
copy (select *
      from parquet_scan('./benchmarks/artifacts/tpch_1/lineitem/part-0.parquet')
      order by random()
      limit 1000)
  to 'testdata/iceberg/source_data/lineitem.parquet';
```

### Local tests

The following command can be used to test local iceberg tables:

```
$ cargo test --test sqllogictests -- 'sqllogictests_iceberg/local'
```

## PRQL integration tests

The directory `prql_integration` contains data from the [PRQL
repo](https://github.com/PRQL/prql/tree/main/crates/prql-compiler/tests/integration/data/chinook).

There's nothing specific to PRQL in these test files as they're just CSVs, but
they're copied in such that we can have PRQL integration tests run in this repo
using our SLT framework.

## Azure storage container

The storage container (bucket) in Azure was bootstrapped via the
[azcopy](https://github.com/Azure/azure-storage-azcopy) utility.

```shell
azcopy copy \
  'https://storage.cloud.google.com/glaredb-test' \
  'https://glaredbtest.blob.core.windows.net/glaredb-test?<sas-token>' \
  --recursive=true
```

Running this command should copy everything from the GCS bucket into the storage
container.

Requirements:
- `GOOGLE_APPLICATION_CREDENTIALS` env var pointing to a service account file.
  Note this can't be application default credentials.
- `<sas-token>` needs to be replaced with a token with "Create", "Add", and
  "Write" permissions. This can be done through the azure dashboard.

(Untested): The `sync` command should let us update the contents of the azure
container when we add additional test data to the gcs bucket.
