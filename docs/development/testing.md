---
title: Testing
---

# Testing

Review [Building From Source](./building.md) for build prerequisites.

We utilize both unit tests and a form of integration tests called [SQL Logic
Tests](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).

## Unit tests

Unit tests should be written to test code paths that cannot easily be tested
through SQL.

Running unit tests: 

```shell
$ cargo test -- --skip slt/
```

This will run all tests across all crates, while skipping the SQL Logic Tests.

## SQL logic tests

SQL Logic Tests allow for running and checking the output of a suite of SQL
queries. SQL Logic Tests (SLTs) should be written for every new feature and
function added.

Running SLTs (standard):

```shell
$ cargo test slt/standard
```

> Note there's currently a couple SLTs that need to be fixed.
> `scripts/run_slt_standard.sh` can be used to skip the failing tests.

All SLTs testing core features should be placed in the `slt/standard` directory.
SLTs for testing features outside of core should be placed in a relevant
subdirectory of `slt` (e.g. `slt/tpch_gen` for the `tpch_gen` extension) and the
path configured in `test_bin/integration_slt.rs`.

`integration_slt.rs` contains engine setup objects responsible for configuring
sessions for each test. For example, the engine setup for S3 will read the
`AWS_KEY` and `AWS_SECRET` environment variables to enable variable replacement
in the tests themselves.

## Miri

A subset of unit tests are run with [Miri](https://github.com/rust-lang/miri) to
catch incorrect `unsafe` usage.

This subset can be ran with:

```shell
$ ./scripts/run_miri.sh
```

All unit tests in `glaredb_core` must pass Miri.

<!-- TODO: Wasm and python testing setups -->
