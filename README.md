# GlareDB

Repository for the core GlareDB database.

## Structure

- **crates**: All rust crates for the database. 
- **scripts**: Miscellaneous scripts for CI and whatnot.
- **testdata**: Data that can be used for testing the system, including TPC-H queries.

## Running

An in-memory version of GlareDB can be started with the following command:

``` shell
cargo run --bin glaredb -- -v server
```

By default, this will start GlareDB on port 6543. All on-disk files will be
stored in a temporary directory.

To connect, use any Postgres compatible client. E.g. `psql`:

``` shell
psql "host=localhost dbname=glaredb port=6543 password=glaredb"
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)
