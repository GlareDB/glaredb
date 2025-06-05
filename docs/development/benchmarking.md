---
title: Benchmarking
order: 3
---

# Benchmarking

The GlareDB repository contains a benchmarking harness (`bench_runner`) for
running a variety of benchmark queries against the system, including ClickBench,
and TPC-H at scale factors 1, 10, 50, and 100.

Review [Building From Source](./building.md) for build prerequisites when
attempting to run `bench_runner`.

## Running a benchmark query

`bench_runner` uses file based filtering to determine which query or queries to
run. All benchmark queries live in the `bench` directory, and end with a
`.bench` extension.

To run a single query, provide a path relative to the root of the repo. For
example, to run `bench/micro/many_base_relation_join_8.bench`:

```shell
cargo bench --bench bench_runner -- bench/micro/many_base_relation_join_8.bench
```

This will first compile `bench_runner` in release mode, then run the benchmark
query some number of times (default 3) and output the average, min, and max
timings for the run:

```text
test [micro] bench/micro/many_base_relation_join_8.bench ... bench
      avg:      16,027 micros,  min:      13,303 micros,  max:      20,403 micros
```

To run a suite a of benchmarks, provide a directory as the filter instead of a
path to a file. For example, to run all micro benchmarks:

```shell
cargo bench --bench bench_runner -- bench/micro
```

This will output average, min, and max timings for each benchmark inlcuded in
the filter.

## Benchmark data

Some benchmarks (TPC-H, ClickBench) require external datasets.
`scripts/bench_download_tpch_data.sh` and
`scripts/bench_download_clickbench_data.sh` can be used to fetch the required
data. These scripts pull from a public GCS bucket, and will place the data in
the correct location.

## Automated benchmarks

Benchmarks for GlareDB are ran daily on a GCP machine using `bench_runner`. The
steps used for running the benchmarks can be found in `scripts/bench_gcp.sh`.

Results of each run are uploaded to a public GCS bucket as TSV files. To get a
list of files, you can use `glob` against the bucket:

```sql
SELECT *
FROM glob('gs://glaredb-bench/results/main/**')
ORDER BY filename DESC;
```

And we can query individual results for a run using `read_csv`:

```sql
SELECT *
FROM read_csv('gs://glaredb-bench/results/main/1749063840/c4-standard-32/results-tpch-parquet-sf-100.tsv');
```
