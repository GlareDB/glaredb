#!/usr/bin/env bash

set -e

python -m venv ./.venv

export VIRTUAL_ENV="${PWD}/.venv"
export PATH="$VIRTUAL_ENV/bin:$PATH"

pip install maturin

maturin develop --manifest-path ./crates/glaredb_python/Cargo.toml

pip install pytest

pytest ./crates/glaredb_python/tests
