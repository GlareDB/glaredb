#!/usr/bin/env bash

set -e

reporoot="$(git rev-parse --show-toplevel)"

mkdir -p "${reporoot}/benchmarks/artifacts/clickbench/"
wget "https://datasets.clickhouse.com/hits_compatible/athena/hits.parquet" \
     -O "${reporoot}/benchmarks/artifacts/clickbench/hits.parquet"
