# GlareDB

Repository for the core GlareDB database.

## Structure

- **crates**: All rust crates for the database. 
- **scripts**: Miscellaneous scripts for CI and whatnot.
- **testdata**: Data that can be used for testing the system, including TPC-H queries.

## Examples

Crates can include examples that may be useful to run during development for a
quick feedback loop.

For example, the `sqlrel` crate contains a `logicalplan` example that reads sql
queries from a file and outputs the logical plan.

``` shell
cargo run --example logicalplan testdata/tpch/1.sql
```

## Testing

Tests can be ran via cargo.

``` shell
cargo test
```

