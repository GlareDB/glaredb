#!/usr/bin/env bash

# Script for running tests using miri.
#
# It's not expected that all tests can pass miri (unsupported features) so we
# need to be a bit more granular with which tests to run.

cargo +nightly miri test -p rayexec_execution

