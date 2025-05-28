#!/usr/bin/env bash
#
# Downloads the clickbench hits file(s) from our GCS bucket. The parquet files
# in the bucket are are the exact same as the ones provide by ClickHouse.

set -eu
set -o pipefail

repo_root=$(git rev-parse --show-toplevel)

pushd "$repo_root"

case "$1" in
    single)
        mkdir -p ./bench/data/clickbench/
        pushd ./bench/data/clickbench/
        wget --continue https://storage.googleapis.com/glaredb-bench/data/clickbench/hits.parquet
        ;;
    partitioned)
        mkdir -p ./bench/data/clickbench/partitioned/
        pushd ./bench/data/clickbench/partitioned/
        seq 0 99 | xargs -P100 -I{} bash -c 'wget --continue https://storage.googleapis.com/glaredb-bench/data/clickbench/partitioned/hits_{}.parquet'
        ;;
    *)
        echo "Invalid argument, expected 'single' or 'partitioned'"
        exit 1
        ;;
esac
