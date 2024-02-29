# Shows help command
default: help
export CARGO_TERM_COLOR := "always"
export PROTOC := justfile_directory() + "/deps/protoc/bin/protoc"
alias py := python
alias js := javascript
alias slt := sql-logic-tests

os_arch := os() + '-' + arch()

VENV := ".venv"
VENV_BIN := VENV / "bin"

# Run benchmarks subcommands. see `benchmarks/justfile` for more details.
bench cmd *args:
  just benchmarks/{{cmd}} {{args}}

# Run py-glaredb subcommands. see `bindings/python/justfile` for more details.
python cmd *args: protoc
  just bindings/python/{{cmd}} {{args}}

# Run js-glaredb subcommands. see `bindings/nodejs/justfile` for more details.
javascript cmd *args: protoc
  just bindings/nodejs/{{cmd}} {{args}}

# Run glaredb server
run *args: protoc
  cargo run --bin glaredb -- {{args}}

# Build glaredb.
build *args: protoc
  cargo build --bin glaredb {{args}}

# Build glaredb with unstable_tokio flag.
#
# Need to run the local command with `--debug-tokio` argument.
build-debug-tokio *args:
  RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=tokio-unstable-target just build {{args}}

run-debug-tokio *args:
  RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=tokio-unstable-target just run {{args}}

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
  just test --workspace {{args}}

# Run doc tests.
doc-tests: protoc
  just test --doc

# Run SQL Logic Tests.
sql-logic-tests *args: build
  just slt-bin {{args}}

slt-bin *args:
  ./target/debug/glaredb sql-logic-tests {{args}}

slt-bin-debug *args:
  ./target/debug/glaredb -v sql-logic-tests {{args}}

# Run SQL Logic Tests over RPC
rpc-tests:
  just slt --protocol=rpc "sqllogictests/*" \
    --exclude "sqllogictests/cloud_instance" \
    --exclude "sqllogictests/functions/cache_external_database_tables" \
    --exclude "sqllogictests/functions/kdl" \
    --exclude "sqllogictests/functions/postgres"

#  Check formatting.
fmt-check: protoc
  cargo +nightly fmt --check

# Apply formatting.
fmt *args: protoc
  cargo +nightly fmt {{args}}

# Run clippy.
clippy: protoc
  cargo clippy --all --all-features -- --deny warnings

# combined target for all lint
lint: clippy fmt-check

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


# Installs python dependencies for testing
venv:
  python3 -c "import virtualenv" || python3 -m pip --quiet install virtualenv
  python3 -m virtualenv .venv --quiet
  {{VENV_BIN}}/python -m pip install poetry
  {{VENV_BIN}}/poetry -C tests install

# Runs pytest in the tests directory.
pytest *args:
  {{VENV_BIN}}/poetry -C tests lock --no-update
  {{VENV_BIN}}/poetry -C tests run pytest --rootdir={{invocation_directory()}}/tests {{ if args == "" {'tests'} else {args} }}

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
