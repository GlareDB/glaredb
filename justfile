# Shows help command
default: help
export CARGO_TERM_COLOR := "always"
export PROTOC := justfile_directory() + "/deps/protoc/bin/protoc"
alias py := python
alias js := javascript

alias slt := sql-logic-tests

os_arch := os() + '-' + arch()


# Run benchmarks subcommands. see `benchmarks/justfile` for more details.
bench cmd *args:
  just benchmarks/{{cmd}} {{args}}

# Run py-glaredb subcommands. see `py-glaredb/justfile` for more details.
python cmd *args: protoc
  just bindings/python/{{cmd}} {{args}}

# Run js-glaredb subcommands. see `js-glaredb/justfile` for more details.
javascript cmd *args: protoc
  just bindings/nodejs/{{cmd}} {{args}}

# Run glaredb server
run *args: protoc
  cargo run --bin glaredb -- {{args}}

# Build glaredb.
build *args: protoc
  cargo build --bin glaredb {{args}}


# A zip archive will be placed in `target/dist` containing the release binary.

# Build the dist binary for release. The target can be overridden by passing in a target triple.
dist triple=target_triple: protoc
  #!/usr/bin/env bash
  set -euo pipefail
  just build --release --target {{triple}}
  src_path="target/{{triple}}/release/{{executable_name}}"
  dest_path="target/dist/glaredb-{{triple}}.zip"
  mkdir -p target/dist
  cargo xtask zip --src $src_path --dst $dest_path

# Run tests with arbitrary arguments.
test *args: protoc
  cargo test {{args}}

# Run unit tests.
unit-tests *args: protoc
  just test --lib --bins {{args}}

# Run doc tests.
doc-tests: protoc
  just test --doc

# Run SQL Logic Tests.
sql-logic-tests *args: protoc
  just test --test sqllogictests -- {{args}}

# Run SQL Logic Tests over RPC
rpc-tests: protoc
  just sql-logic-tests -v --rpc-test \
    'sqllogictests/cast/*' \
    'sqllogictests/cte/*' \
    'sqllogictests/functions/delta_scan' \
    'sqllogictests/functions/generate_series' \
    'sqllogictests/functions/version' \
    'sqllogictests/joins/*' \
    'sqllogictests/topn/*' \
    'sqllogictests/window/*' \
    'sqllogictests/aggregates' \
    'sqllogictests/alter' \
    'sqllogictests/create_table' \
    'sqllogictests/credentials' \
    'sqllogictests/csv' \
    'sqllogictests/debug' \
    'sqllogictests/delete' \
    'sqllogictests/demo_pg' \
    'sqllogictests/drop' \
    'sqllogictests/explain' \
    'sqllogictests/external_table' \
    'sqllogictests/http' \
    'sqllogictests/infer' \
    'sqllogictests/information_schema' \
    'sqllogictests/metabase' \
    'sqllogictests/name' \
    'sqllogictests/object_names' \
    'sqllogictests/pg_catalog' \
    'sqllogictests/rpc' \
    'sqllogictests/schema' \
    'sqllogictests/search_path' \
    'sqllogictests/select' \
    'sqllogictests/simple' \
    'sqllogictests/table' \
    'sqllogictests/temp_table' \
    'sqllogictests/time' \
    'sqllogictests/tunnels' \
    'sqllogictests/update' \
    'sqllogictests/vars' \
    'sqllogictests/views' \
    'sqllogictests/virtual_catalog' \
    'sqllogictests/xlsx' \
    'sqllogictests/prql' \
    'sqllogictests/describe_rpc' \
    'sqllogictests/allowed_operations'

#  Check formatting.
fmt-check: protoc
  cargo fmt -- --check

# Apply formatting.
fmt *args: protoc
  cargo fmt {{args}}

# Run clippy.
clippy: protoc
  cargo clippy --all --all-features -- --deny warnings

# apply linting & clippy fixes.
fix: protoc
  cargo clippy --fix --all --all-features --allow-staged --allow-dirty
  cargo fix --all --allow-staged  --allow-dirty
  just fmt --all

# Displays help message.
help:
  @just --list

protoc:
  #!/bin/bash
  if ! $PROTOC --version > /dev/null; then
    echo "Installing protoc..." && \
    curl -L {{protoc_url}} -o protoc.zip && \
    rm -rf deps/protoc && \
    mkdir -p deps/ && \
    unzip -o protoc.zip -d deps/protoc && \
    rm protoc.zip
  fi


# private helpers below
# ---------------------

default_target_triple := if os_arch == "macos-x86_64" {
  "x86_64-apple-darwin"
} else if os_arch == 'macos-aarch64' {
  "aarch64-apple-darwin"
} else if os_arch == "linux-x86_64" {
  "x86_64-unknown-linux-gnu"
} else if os_arch == "linux-aarch64" {
  "aarch64-unknown-linux-gnu"
} else if os_arch == "windows-x86_64" {
  "x86_64-pc-windows-msvc"
} else {
  error("Unsupported platform: " + os_arch)
}

target_triple:= env_var_or_default("DIST_TARGET_TRIPLE", default_target_triple)

protoc_url := if os_arch == "macos-x86_64" {
  "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-osx-universal_binary.zip"
} else if os_arch == 'macos-aarch64' {
  "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-osx-universal_binary.zip"
} else if os_arch == "linux-x86_64" {
  "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-x86_64.zip"
} else if os_arch == "linux-aarch64" {
  "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-linux-aarch_64.zip"
} else if os_arch == "windows-x86_64" {
  "https://github.com/protocolbuffers/protobuf/releases/download/v23.1/protoc-23.1-win64.zip"
} else {
  error("Unsupported platform: " + os_arch)
}

executable_name:= if os() == "windows" {"glaredb.exe"} else {"glaredb"}
