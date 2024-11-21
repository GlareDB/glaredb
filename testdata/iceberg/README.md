# Iceberg testdata

Generating iceberg data is done with the `generate_iceberg.py` script. This
script requires `pyspark` as well as a [Spark runtime
jar](https://iceberg.apache.org/releases/). The jar should be placed in this
directory (jars are in gitignore, so don't worry about accidentally checking it
in).

Then just call the script with python:

```
$ python generate_iceberg.py
```

This will generate various iceberg tables in `./iceberg/tables-v2` using source
data from parquet files in `./iceberg/source_data`.

The script accepts an argument `--format-version` which defaults to `2`. You can
generate data for format version `1` using:

```
$ python generate_iceberg.py --format-version 1
```

Test data has also been uploaded to GCS and S3 with the following commands:

```
$ gsutil cp -r iceberg/ gs://glaredb-test/iceberg
$ aws s3 cp --recursive iceberg/ s3://glaredb-test/iceberg
```

## Source data

Source data was generated with sql queries like the following:

```sql
copy (select *
      from parquet_scan('./benchmarks/artifacts/tpch_1/lineitem/part-0.parquet')
      order by random()
      limit 1000)
  to 'testdata/iceberg/source_data/lineitem.parquet';
```

