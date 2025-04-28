#!/usr/bin/env bash

set -e

repo_root=$(git rev-parse --show-toplevel)
script_dir=$(dirname "$0")

if [[ "$(basename "$repo_root")" == "glaredb" ]]; then
    # Inside glaredb repo, build from source.
    cargo build --release --bin glaredb
    cp "${repo_root}/target/release/glaredb" "${script_dir}/glaredb"
else
    # Not in glaredb repo, use prebuilt binary.
    export GLAREDB_INSTALL_DIR="${script_dir}"
    export GLAREDB_VERSION="v0.10.14"
    curl -fsSL https://glaredb.com/install.sh | sh
fi

# Get the data.
mkdir -p "${script_dir}/data"
pushd "${script_dir}/data"
case "$1" in
    single)
        wget --continue https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits_compatible/athena/hits.parquet
        ;;
    partitioned)
        seq 0 99 | xargs -P100 -I{} bash -c 'wget --continue https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet'
        ;;
    *)
        echo "Invalid argument to 'benchmark.sh', expected 'single' or 'partitioned'"
        exit 1
        ;;
esac
popd

# Ensure working directory in the script dir. The view that gets created uses a
# relative path.
pushd "${script_dir}"
trap 'popd' EXIT

./run.sh "$@"
