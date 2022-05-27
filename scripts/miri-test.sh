#!/usr/bin/env bash

# Execute miri tests in the project. If no filter arg is provided, all tests
# will be ran.
#
# Installs the latest nightly version of rust through `rustup` that contains
# miri.

set -e

MIRI_NIGHTLY=nightly-$(curl -s https://rust-lang.github.io/rustup-components-history/x86_64-unknown-linux-gnu/miri)
echo "Using nightly version: ${MIRI_NIGHTLY}"

rustup toolchain install "${MIRI_NIGHTLY}"
rustup +"${MIRI_NIGHTLY}" component add miri

cargo clean

# miri-disable-isolation is set due running envlogger in tests and
# `clock_gettime` (which envlogger uses deep down) is not supported yet.
export MIRIFLAGS="-Zmiri-disable-isolation"

# Prompts to install things as necessary. Will assume yes when the CI env var is
# set.
cargo +"${MIRI_NIGHTLY}" miri test "$@"
