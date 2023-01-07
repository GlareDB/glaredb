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

When prompted for a password, any password will do[^1].

## Code Overview

### Binaries

The `glaredb` binary lives in the `glaredb` crate. This one binary is able to
start either the GlareDB server itself (the actual database), or the proxy[^2].

### Postgres wire protocol

We aim to be Postgres wire protocol compatible. The implementation of the
protocol, along with the proxying code, lives in the `pgsrv` crate.

### SQL Execution

The `sqlexec` crate provides most of the code related to SQL execution,
including session states and catalogs. The functionality exposed through
`sqlexec` maps closely to what's needed within `pgsrv`.

### Data sources

Data sources are external sources that GlareDB can hook into via `CREATE
EXTERNAL TABLE ...` calls. Each data source lives in its own `datasource_*`
crate.

## Logging and Tracing

We use the [tracing](https://docs.rs/tracing/latest/tracing/) library for our logging needs. When running locally,
these logs are output in a human-readable format. When running through Cloud,
logs are output in a JSON format which makes them searchable in GCP's logging
dashboard.

For ease of debugging, each external connection that comes in is given a unique
connection ID (UUID), and each Postgres message we encounter from that
connection creates a new span including that connection ID and the message that
we're processing. What that means is for most logs in the system, there will be
an accompanying connection ID for each log as seen below:

``` text
2023-01-06T19:43:13.840561Z DEBUG glaredb-thread-7 ThreadId(09) glaredb_connection{conn_id=2e881011-b649-4490-a5e2-1f086f7cee2a}:pg_protocol_message{name="query"}: sqlexec::planner: crates/sqlexec/src/planner.rs:27: planning sql statement statement=SELECT 1
2023-01-06T19:43:13.844780Z TRACE glaredb-thread-7 ThreadId(09) glaredb_connection{conn_id=2e881011-b649-4490-a5e2-1f086f7cee2a}:pg_protocol_message{name="query"}: pgsrv::codec::server: crates/pgsrv/src/codec/server.rs:48: sending message msg=RowDescription([FieldDescription { name: "Int64(1)", table_id: 0, col_id: 0, obj_id: 0, type_size: 0, type_mod: 0, format: 0 }])
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

[^1]: GlareDB currently allows any password. Access restriction is done within
    the `pgsrv` proxy.

[^2]: Eventually the proxy entrypoint will be split out into its own binary. See
    <https://github.com/GlareDB/glaredb/issues/367>
