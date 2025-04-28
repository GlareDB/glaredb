#!/usr/bin/env bash

set -e

# If in glaredb repo, build from source.
cargo build --release --bin glaredb

# Or alternatively download prebuilt binary.
# script_dir=$(dirname "$0")
# export GLAREDB_INSTALL_DIR="${script_dir}"
# export GLAREDB_VERSION="v0.10.14"
# curl -fsSL https://glaredb.com/install.sh | sh

# Get the (unpartitioned) data
wget --continue https://clickhouse-public-datasets.s3.eu-central-1.amazonaws.com/hits_compatible/athena/hits.parquet
