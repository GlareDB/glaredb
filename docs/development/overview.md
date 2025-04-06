---
title: Overview
---

# Development overview

## Building

### CLI

Building the CLI from source requires [protoc] to be in your path.

```sh
$ cargo build --release --bin glaredb
```

The binary will be located at `./target/release/glaredb`

### Python

> `rayexec` is an old name for the core engine. References will be replaced with
> `glaredb` soon.

Building the python bindings from source requires [protoc] and [maturin] to be
in your path, as well as a [venv] set up.

Create the virtual env:

```sh
$ python -m venv crates/rayexec_python/venv
```

Activate the virtual env:

```sh
$ cd crates/rayexec_python
$ source ./venv/bin/activate
# OR
$ export VIRTUAL_ENV="<path-to-venv-dir>"
$ export PATH="$VIRTUAL_ENV/bin:$PATH"
```

Build the release bindings (assuming current directory is
`./crates/rayexec_python`):

```sh
maturin build --release --out dist
```

The wheel will be located at `./dist/rayexec-...`.

### Wasm

Building the wasm bindings requires [protoc] in your path and [wasm-pack]
installed.

Build the release bindings:

```sh
$ wasm-pack build crates/glaredb_wasm/ --scope glaredb
```

The wasm blobs and related javascript will be located at `./crates/glaredb_wasm/pkg`.

## Testing

We utilize both unit tests and a form of integration tests called [SQL Logic
Tests].

Running tests requires that [protoc] be in your path.

### Unit tests

Unit tests should be written to test code paths that cannot easily be tested
through SQL.

Running unit tests: 

```sh
$ cargo test -- --skip slt/
```

### SQL Logic Tests

[SQL Logic Tests] allow for running and checking the output of a suite of SQL
queries. SQL Logic Tests (SLTs) should be written for every new feature and
function added.

Running SLTs (standard):

```sh
$ cargo test slt/standard
```

> Note there's currently a couple SLTs that need to be fixed.
> `./scripts/run_slt_standard.sh` can be used to skip the failing tests.

All SLTs testing core features should be placed in the `./slt/standard`
directory. SLTs for testing features outside of core should be placed in a
relevent subdirectory of `./slt` (e.g. `./slt/tpch_gen` for the `tpch_gen`
extension) and an integration test runner added to `./test_bin`.

### Miri

A subset of unit tests are run with [Miri] to catch incorrect `unsafe` usage.

This subset can be ran with:

```sh
$ ./scripts/run_miri.sh
```

All unit tests in `glaredb_core` must pass Miri.

## Code style

### Formatting and linting

[rustfmt] (nightly) and [clippy] are used for formatting and linting
respectively.

Check formatting:

```sh
$ cargo +nightly fmt --check
```

Run clippy:

```sh
$ cargo clippy --all --all-features -- --deny warnings
```

[SQL Logic Tests]: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
[protoc]: https://protobuf.dev/installation/
[wasm-pack]: https://github.com/rustwasm/wasm-pack
[venv]: https://docs.python.org/3/library/venv.html
[maturin]: https://github.com/PyO3/maturin
[Miri]: https://github.com/rust-lang/miri
[rustfmt]: https://github.com/rust-lang/rustfmt
[clippy]: https://github.com/rust-lang/rust-clippy
