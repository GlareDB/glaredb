---
title: Building
---

# Building From Source

## Build Prerequisites

GlareDB is developed primarily in Rust, and building from source requires that a
Rust toolchain be setup, and accessible in your path. The easiest way to get
started with Rust is by using [rustup]. All distributed release binaries of
GlareDB use the latest stable Rust toolchain.

[protoc] is also required to be in your path when building GlareDB.

## CLI

Building the CLI from source is just a simple `cargo build`:

```shell
$ cargo build --release --bin glaredb
```

The binary will be located at `./target/release/glaredb`

The `--release` flag may be omitted to make the build quite a bit quicker.
However this results in a larger and slower binary, and should only be used when
during development. When benchmarking, always pass the `--release` flag.

## Python

In addition to the above prerequisites, building the Python bindings requires
[maturin] to be in your path, as well asn a [venv] set up.

To create the virtual env:

```shell
$ python -m venv crates/glaredb_python/venv
```

Activate the virtual env:

```shell
$ cd crates/glaredb_python
$ source ./venv/bin/activate
# OR
$ export VIRTUAL_ENV="<path-to-venv-dir>"
$ export PATH="$VIRTUAL_ENV/bin:$PATH"
```

Build the release bindings (assuming the current directory is
`./crates/glaredb_python`):

```shell
$ maturin build --release --out dist
```

The wheel will be located at `./dist/glaredb-...`.

`release` may be omitted for a faster build, but this should only be done during
development.

## WebAssembly

In addition to the above prerequisites, building the WebAssembly bindings
requires [wasm-pack] to be in your path.

Build the release bindings:

```shell
$ wasm-pack build crates/glaredb_wasm/ --scope glaredb
```

The wasm blobs and related javascript will be located at `./crates/glaredb_wasm/pkg`.

The `--dev` flag may be use for a faster build. This should only be done during
development.

## Release vs Debug Builds

Compiling binaries with the `--release` flag (or for WebAssembly, omitting the
`--dev` flag) will result in the fastest binary and typically smallest binary.

Debug binaries are slower and larger. However they're quick to build, and have
additional runtime assertions enabled. When debugging an issue, reproducing it
with a debug binary often times provides more information about what went wrong.
All tests (unit tests and SQL Logic Tests) are ran in debug mode.

[wasm-pack]: https://github.com/rustwasm/wasm-pack
[maturin]: https://github.com/PyO3/maturin
[protoc]: https://protobuf.dev/installation/
[rustup]: https://rustup.rs/
