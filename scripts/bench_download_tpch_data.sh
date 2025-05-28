#!/usr/bin/env bash
#
# Downloads tpch data (parquet files).
#
# Data generated with <https://github.com/clflushopt/tpchgen-rs>

set -eu
set -o pipefail

sf="${1:?Missing scale factor (Usage: $0 <scale-factor>)}"

repo_root=$(git rev-parse --show-toplevel)

pushd "$repo_root"

mkdir -p "./bench/data/tpch/${sf}"
pushd "./bench/data/tpch/${sf}"

wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/customer.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/lineitem.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/nation.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/orders.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/part.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/partsupp.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/region.parquet"
wget --continue --quiet "https://storage.googleapis.com/glaredb-bench/data/tpch/${sf}/supplier.parquet"
