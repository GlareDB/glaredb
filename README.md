<!-- markdownlint-disable no-inline-html -->
<!-- markdownlint-disable first-line-h1 -->
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

Data exists everywhere: your laptop, [Postgres], [Snowflake] and as
[files in S3]. It exists in various formats such as Parquet, CSV and JSON.
Regardless, there will always be multiple steps spanning several destinations to
get the insights you need.

**GlareDB is designed to query your data wherever it lives using SQL that you
already know.**

## Install

Install/update `glaredb` in the **current directory**:

```shell
curl https://glaredb.com/install.sh | sh
```

It may be helpful to install the binary in a location on your `PATH`. For
example, `~/.local/bin`.

If you prefer manual installation, download, extract and run the GlareDB binary
from a release in our [releases page].

## Supported data sources

| Source                 | Read | Write | Table Function | External Table | External Database | Supported Object Stores             |
| ---------------------- | :--: | :---: | :------------: | :------------: | ----------------- | ----------------------------------- |
| **Databases**          |  --  |  --   |       --       |       --       | --                | --                                  |
| MySQL                  |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| PostgreSQL             |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| Microsoft SQL Server   |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| MongoDB                |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| Snowflake              |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| BigQuery               |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| Cassandra/ScyllaDB     |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| Clickhouse             |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| MariaDB (_via mysql)_  |  ✅  |  ✅   |       ✅       |       ✅       | ✅                | ➖                                  |
| DuckDB                 |  🚧  |  🚧   |       🚧       |       🚧       | 🚧                | ➖                                  |
| Oracle                 |  🚧  |  🚧   |       🚧       |       🚧       | 🚧                | ➖                                  |
| SQLite                 |  🚧  |  🚧   |       🚧       |       🚧       | 🚧                | ➖                                  |
| **File Formats**       |  --  |  --   |       --       |       --       | --                | --                                  |
| CSV                    |  ✅  |  ✅   |       ✅       |       ✅       | ➖                | HTTP, S3, Google, Azure, Local File |
| Newline Delimited JSON |  ✅  |  ✅   |       ✅       |       ✅       | ➖                | HTTP, S3, Google, Azure, Local File |
| Apache Parquet         |  ✅  |  ✅   |       ✅       |       ✅       | ➖                | HTTP, S3, Google, Azure, Local File |
| Apache Arrow           |  ✅  |  ✅   |       ✅       |       ✅       | ➖                | HTTP, S3, Google, Azure, Local File |
| Delta                  |  ✅  |  ✅   |       ✅       |       ✅       | ➖                | HTTP, S3, Google, Azure, Local File |
| BSON                   |  ✅  |  🚧   |       ✅       |       ✅       | ➖                | Local File                          |
| Iceberg                |  ✅  |  🚧   |       ✅       |       ✅       | ➖                | HTTP, S3, Google, Azure, Local File |
| Lance                  |  ✅  |  🚧   |       ✅       |       🚧       | ➖                | HTTP, S3, Google, Azure, Local File |
| Excel                  |  ✅  |  🚧   |       ✅       |       🚧       | ➖                | Local File                          |
| JSON                   |  🚧  |  🚧   |       🚧       |       🚧       | ➖                | 🚧                                  |
| Apache Avro            |  🚧  |  🚧   |       🚧       |       🚧       | ➖                | 🚧                                  |
| Apache ORC             |  🚧  |  🚧   |       🚧       |       🚧       | ➖                | 🚧                                  |

✅ = Supported
➖ = Not Applicable
🚧 = Not Yet Supported

## Getting started

After [Installing](#install), get up and running with:

- [**CLI**](#local-cli)
  - [Run GlareDB server](#local-server)
- [**Hybrid Execution**](#hybrid-execution)
- [**Python**](#using-glaredb-in-python)

### Local CLI

To start a local session, run the binary:

```shell
./glaredb
```

Or, you can execute SQL and immediately return (**try it out!**):

```shell
# Query a CSV on Hugging Face
./glaredb --query "SELECT * FROM \
'https://huggingface.co/datasets/fka/awesome-chatgpt-prompts/raw/main/prompts.csv';"
```

To see all options use `--help`:

```sh
./glaredb --help
```

### Hybrid Execution

1. Sign up at <https://console.glaredb.com> for a **free** fully-managed
   deployment of GlareDB
2. Copy the connection string from GlareDB Cloud, for example:

   ```shell
   ./glaredb --cloud-url="glaredb://user:pass@host:port/deployment"
   # or
   ./glaredb
   > \open "glaredb://user:pass@host:port/deployment
   ```

Read our [announcement on Hybrid Execution] for more information.

### Using GlareDB in Python

1. Install the official [GlareDB Python library]

   ```shell
   pip install glaredb
   ```

2. Import and use `glaredb`.

   ```python
   import glaredb
   con = glaredb.connect()
   con.sql("select 'hello world';").show()
   ```

To use **Hybrid Execution**, sign up at <https://console.glaredb.com> and
use the connection string for your deployment. For example:

```python
import glaredb
con = glaredb.connect("glaredb://user:pass@host:port/deployment")
con.sql("select 'hello hybrid exec';").show()
```

GlareDB work with [Pandas] and [Polars] DataFrames out of the box:

```python
import glaredb
import polars as pl

df = pl.DataFrame(
    {
        "A": [1, 2, 3, 4, 5],
        "fruits": ["banana", "banana", "apple", "apple", "banana"],
        "B": [5, 4, 3, 2, 1],
        "cars": ["beetle", "audi", "beetle", "beetle", "beetle"],
    }
)

con = glaredb.connect()

df = con.sql("select * from df where fruits = 'banana'").to_polars();

print(df)
```

### Local server

The `server` subcommand can be used to launch a server process for GlareDB:

```shell
./glaredb server
```

To see all options for running in server mode, use `--help`:

```sh
./glaredb server --help
```

When launched as a server process, GlareDB can be reached on port `6543` using a
Postgres client. The following example uses `psql` to connect to a locally
running server:

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

Check out the docs to learn about all [supported data sources]. Many data
sources can be connected to the same GlareDB instance.

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

## Acknowledgements

GlareDB is proudly powered by [Apache Datafusion](https://arrow.apache.org/datafusion/) and [Apache Arrow](https://arrow.apache.org/). We are grateful for the work of the Apache Software Foundation and the community around these projects.

[Postgres]: https://docs.glaredb.com/docs/data-sources/supported/postgres.html
[Snowflake]: https://docs.glaredb.com/docs/data-sources/supported/snowflake.html
[files in S3]: https://docs.glaredb.com/docs/data-sources/supported/s3
[releases page]: https://github.com/GlareDB/glaredb/releases
[announcement on Hybrid Execution]: https://glaredb.com/blog/hybrid-execution
[GlareDB Python library]: https://pypi.org/project/glaredb/
[Pandas]: https://github.com/pandas-dev/pandas
[Polars]: https://github.com/pola-rs/polars
[supported data sources]: https://docs.glaredb.com/docs/data-sources/supported/
