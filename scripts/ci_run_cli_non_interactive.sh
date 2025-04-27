#!/usr/bin/env bash

# Run some non-interactive queries to make sure they work.

# Simple text output.
cargo run --bin glaredb -- "SELECT '◊◊◊◊◊'"

# Long text output, default truncated.
cargo run --bin glaredb -- "SELECT * FROM generate_series(1, 1000)"
