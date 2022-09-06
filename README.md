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

## Testing

### Unit Tests

Unit tests attempt to test small parts of the system. These can be run as you
would expect with cargo:

``` shell
cargo test
```

When writing unit tests, aims to keep the scope small with a minimal amount of
setup required.

### SQL Logic Tests

SQL logic tests run end-to-end tests that execute actual SQL queries against a
running database. These tests can be found in `testdata/sqllogictest`. Running
these tests can be done with:

``` shell
cargo run --bin slt_runner -- --keep-running testdata/sqllogictests/*.slt
```

The `--keep-running` flag will keep the GlareDB server up to allow for
additional debugging.

An example SQL logic test is as follows:

``` text
query III nosort
select * from (values (1, 2, 3), (3, 4, 5))
----
1 2 3
3 4 5
```

`query` indicates that the SQL query on the next line is expected to return
values. `III` indicates that three columns of **I**ntegers will be returned. `R`
can be used for **R**eals, and `T` can be used for **T**ext. `nosort` instructs
that no sorting be done on the client-side (an `ORDER BY` should be used in most
cases to allow for accurate comparisons with expected results). Everything after
the `----` is the expected results for the query.

More details on the [sqllogictest wiki page](https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki).

## Contributing

All commits to `main` should first go through a PR. All CI checks should pass
before merging in a PR. Since we squash merge, PR titles should follow
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).
