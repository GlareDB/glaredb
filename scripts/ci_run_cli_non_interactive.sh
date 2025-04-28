#!/usr/bin/env bash

# Run some non-interactive queries to make sure they work.

set -ex

groot=$(git rev-parse --show-toplevel)

# Version
cargo run --bin glaredb -- --version

# SQL Commands

cargo run --bin glaredb -- -c "SELECT '◊◊◊◊◊'"
cargo run --bin glaredb -- -c "SELECT * FROM generate_series(1, 1000)"

cargo run --bin glaredb -- -c "SELECT 'one'; SELECT 'two'"
cargo run --bin glaredb -- -c "SELECT 'one'" -c "SELECT 'two'"

# Dot comamands

cargo run --bin glaredb -- -c ".help"
cargo run --bin glaredb -- -c ".databases"

# Files

cargo run --bin glaredb -- -f "${groot}/testdata/cli/create_view_v1.sql" -f "${groot}/testdata/cli/query_view_v1.sql"

# Init file

cargo run --bin glaredb -- --init "${groot}/testdata/cli/create_view_v1.sql" -f "${groot}/testdata/cli/query_view_v1.sql"
cargo run --bin glaredb -- --init "${groot}/testdata/cli/create_view_v1.sql" -c "SELECT min(a), max(a) FROM v1"
cargo run --bin glaredb -- --init "${groot}/testdata/cli/create_view_v1.sql" -c ".timer on" -c ".box ascii" -c "SELECT min(a), max(a) FROM v1"
