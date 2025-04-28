#!/usr/bin/env bash

set -e

repo_root=$(git rev-parse --show-toplevel)
script_dir=$(dirname "$0")

if [[ "$(basename "$repo_root")" == "glaredb" ]]; then
    # Inside glaredb repo, build from source.
    cargo build --release --bin glaredb
    cp "${repo_root}/release/glaredb" "${script_dir}/glaredb"
else
    # Not in glaredb repo, use prebuilt binary.
    export GLAREDB_INSTALL_DIR="${script_dir}"
    export GLAREDB_VERSION="v0.10.14"
    curl -fsSL https://glaredb.com/install.sh | sh
fi

# Get the (unpartitioned) data
wget --continue https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits_compatible/athena/hits.parquet
