#!/usr/bin/env bash

# Script for running tests using miri.
#
# It's not expected that all tests can pass miri (unsupported features) so we
# need to be a bit more granular with which tests to run.
#
# Even if a crate doesn't make use of unsafe, it's still good to run it under
# Miri as the code paths take may end up passing through unsafe code.

set -e

# Core crates.
cargo +nightly miri test -p rayexec_execution

# Data source crates.
cargo +nightly miri test -p rayexec_csv
