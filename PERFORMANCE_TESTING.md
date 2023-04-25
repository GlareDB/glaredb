# Performance Testing

This document describes how we do performance testing in GlareDB. We do not
currently have automated performance tests.

We do not currently have automated performance testing.

## Test data

Performance testing data is stored in the `glaredb-benchmarks` bucket in the
`glaredb-artifacts` project.

### TPC-H

TPC-H data is generated from utilies in the DataFusion repo. See the
[benchmarks](https://github.com/apache/arrow-datafusion/tree/main/benchmarks) directory.

Generating and uploading parquet files can be done via:

```shell
cd benchmarks
./tpch-gen.sh <scale-factor>
cargo run --release --bin tpch -- convert --input ./data --output ./data-<scale-factor> --format parquet
gsutil cp -r ./data-<scale-factor>/* gs://glaredb-benchmarks/data/tpch/sf<scale-factor>
```

Where `<scale-factor>` is replaced with a number.

The `run-tpch-queries.sh` script can be used to create external tables and
execute predefined queries against these tables.

``` shell
export CONNECTION_STRING="my-connection-string"
export GCP_SERVICE_ACCOUNT_JSON="service-account-json"
export SCALE_FACTOR=10 # Optional, defaults to 1
./scripts/run-tpch-queries.sh
```
