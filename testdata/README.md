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
