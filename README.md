# GlareDB

Repository for the core GlareDB database.

## Structure

- **crates**: All rust crates for the database. 
- **scripts**: Miscellaneous scripts for CI and whatnot.
- **testdata**: Data that can be used for testing the system, including TPC-H queries.

## Running

An in-memory version of GlareDB can be started with the following command:

``` shell
cargo run --bin glaredb -- -vv server --storage memory -b 0.0.0.0:6543
```

To connect, use any Postgres compatible client. E.g. `psql`:

``` shell
psql "host=localhost dbname=glaredb port=6543 password=glaredb"
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
