#!/usr/bin/env bash

# Run example binaries.

set -ex

cargo run --bin example_basic
cargo run --bin example_extension_scalar_function
cargo run --bin example_extension_aggregate_function
