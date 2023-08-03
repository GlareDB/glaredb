<div align="center">
  <a href="https://glaredb.com#gh-light-mode-only">
    <img src="https://docs.glaredb.com/assets/logo.svg" height="44">
  </a>
  <a href="https://glaredb.com#gh-dark-mode-only">
    <img src="https://glaredb.com/logo.svg" height="44">
  </a>
</div>

<!-- Adds some spacing between logo and badges. -->
<p></p>

<div align="center">
<a href="https://docs.glaredb.com"><img src="https://img.shields.io/static/v1?label=docs&message=GlareDB%20Reference&color=55A39B&style=flat-square"></img></a>
<a href="https://github.com/GlareDB/glaredb/releases"><img src="https://img.shields.io/github/v/release/glaredb/glaredb?display_name=tag&style=flat-square"></img></a>
<a href="https://pypi.org/project/glaredb"><img src="https://img.shields.io/pypi/v/glaredb?style=flat-square"</img></a>
<a href="https://twitter.com/glaredb"><img src="https://img.shields.io/twitter/follow/glaredb?color=blue&logo=twitter&style=flat-square"></img></a>
<a href="https://discord.gg/2D7qxC5xkf"><img src="https://img.shields.io/static/v1?label=Chat on Discord&message= &color=360066&style=flat-square"></img></a>
</div>

## About

GlareDB is a database system built for querying and analyzing distributed data
using SQL. Query data directly from [Snowflake], [Postgres], object storage, and
more without moving data around.

## Install

```shell
curl https://glaredb.com/install.sh | sh
```

Or check out [console.glaredb.com](https://console.glaredb.com) for fully managed deployments of GlareDB.
If you prefer manual installation, download, extract and run the GlareDB binary from a release in our
[releases page](https://github.com/GlareDB/glaredb/releases).

## Getting started

Start GlareDB with the `local` subcommand to start a local SQL session:

```shell
./glaredb local
```

Alternatively, the `server` subcommand can be used to launch a server process
for GlareDB:

```shell
./glaredb server
```

When launched as a server process, GlareDB can be reached on port 6543 using a
Postgres client. For example, connected to a local instance of GlareDB using
`psql`:

```shell
psql "host=localhost user=glaredb dbname=glaredb port=6543"
```

## Your first data source

A demo Postgres instance is deployed at `pg.demo.glaredb.com`. Adding this
Postgres instance as data source is as easy as running the following command:

```sql
CREATE EXTERNAL DATABASE my_pg
    FROM postgres
    OPTIONS (
        host = 'pg.demo.glaredb.com',
        port = '5432',
        user = 'demo',
        password = 'demo',
        database = 'postgres',
    );
```

Once the data source has been added, it can be queried using fully qualified
table names:

```sql
SELECT *
FROM my_pg.public.lineitem
WHERE l_shipdate <= date '1998-12-01' - INTERVAL '90'
LIMIT 5;
```

Check out the docs to learn about all [supported data sources](https://docs.glaredb.com/docs/data-sources/supported/). Many
data sources can be connected to the same GlareDB instance.

Done with this data source? Remove it with the following command:

```sql
DROP DATABASE my_pg;
```

## Building from source

Building GlareDB requires Rust/Cargo to be installed. Check out [rustup](https://rustup.rs/) for
an easy way to install Rust on your system.

Running the following command will build a release binary:

```shell
just build --release
```

The compiled release binary can be found in `target/release/glaredb`.

## Docs

Browse GlareDB documentation on our [docs.glaredb.com](https://docs.glaredb.com).

## Contributing

Contributions welcome! Check out [CONTRIBUTING.md](CONTRIBUTING.md) for how to get started.

## License

See [LICENSE](./LICENSE). Unless otherwise noted, this license applies to all files in
this repository.

[Snowflake]: https://docs.glaredb.com/docs/data-sources/supported/snowflake.html
[Postgres]: https://docs.glaredb.com/docs/data-sources/supported/postgres.html
[Supported data sources]: https://docs.glaredb.com/docs/data-sources/supported/

## Acknowledgements

GlareDB is proudly powered by [Apache Datafusion](https://arrow.apache.org/datafusion/) and [Apache Arrow](https://arrow.apache.org/). We are grateful for the work of the Apache Software Foundation and the community around these projects.
