#!/usr/bin/env bash
#
# Publishes library crates to cargo.
#
# Only crates that are usable/useful in the context of using inside other rust
# programs are published.

set -e

# Crates must be specified in an order to ensure all of a crate's dependencies
# are published before it.
crates=(
    glaredb_error
    glaredb_proto
    glaredb_parser

    glaredb_core
    glaredb_http
    glaredb_rt_native

    glaredb_ext_csv
    glaredb_ext_parquet
    glaredb_ext_delta
    glaredb_ext_iceberg
    glaredb_ext_spark
    glaredb_ext_tpch_gen

    glaredb_ext_default
)

for crate in "${crates[@]}"; do
    cargo publish --package "$crate"
done
