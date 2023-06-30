# Build glaredb.
default: build
export CARGO_TERM_COLOR := "always"

alias py := python
os_arch := os() + '-' + arch()

  
# Run py-glaredb subcommands. see `py-glaredb/justfile` for more details.
python cmd *args: protoc
  just py-glaredb/{{cmd}} {{args}}

# Run glaredb server
run *args: protoc
  cargo run --bin glaredb {{args}}

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
  zip -j $dest_path $src_path

# Run tests with arbitrary arguments.
test *args: protoc
  cargo test {{args}}

# Run unit tests.
unittests: protoc
  just test --lib --bins

# Run doc tests.
doctests: protoc
  just test --doc

# Run SQL Logic Tests.
slt *args: protoc
  just test --test sqllogictests {{args}}

#  Check formatting.
fmt-check: protoc
  cargo fmt -- --check

# Apply formatting.
fmt *args: protoc
  cargo fmt *args

# Run clippy.
clippy: protoc
  cargo clippy --all-features -- --deny warnings

# Displays help message.
help: 
  @just --list


# private helpers below
# ---------------------

[private]
protoc:
  #!/bin/bash
  if ! protoc --version > /dev/null; then 
    echo "Installing protoc..." && \
    curl -L {{protoc_url}} -o protoc.zip && \
    rm -rf deps/protoc && \
    mkdir -p deps/ && \
    unzip -o protoc.zip -d deps/protoc && \
    rm protoc.zip
  fi


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