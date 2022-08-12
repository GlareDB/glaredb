# GlareDB

Repository for the core GlareDB database.

## Structure

- **crates**: All rust crates for the database. 
- **scripts**: Miscellaneous scripts for CI and whatnot.
- **testdata**: Data that can be used for testing the system, including TPC-H queries.

## Examples

Crates can include examples that may be useful to run during development for a
quick feedback loop.

For example, the `sqlengine` crate contains a `example` example that reads and
executes a sql query:

``` shell
cargo run --example execute "explain select 1;"
```

## Testing

Tests can be ran via cargo.

``` shell
cargo test
```

## Running

The server and client portion of GlareDB is executed through a single target.

To run the server (using the default port of 6543 for sql queries):

``` shell
cargo run --bin glaredb -- server
```

To connect a client to the server:

``` shell
cargo run --bin glaredb -- client localhost:6543
```

Once connected, a simple REPL will be started for inputting sql queries.
