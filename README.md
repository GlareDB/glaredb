<!-- markdownlint-disable no-inline-html -->
<!-- markdownlint-disable first-line-h1 -->
<div align="center">
  <a href="https://glaredb.com#gh-light-mode-only">
    <img src="https://docs.glaredb.com/assets/logo.svg" height="60q">
  </a>
  <a href="https://glaredb.com#gh-dark-mode-only">
    <img src="https://glaredb.com/logo.svg" height="60">
  </a>
</div>
<br/>
<p align="center">
Data exists everywhere: on laptops, in databases like Postgres and Snowflake, and as files in S3, in various formats including Parquet, CSV, and JSON. Accessing and analyzing this data typically involves multiple steps across different systems.<br/> <br/> <b>GlareDB is designed to query your data wherever it lives using SQL that you
already know.</b>
</p>
<p align="center">
<a href="https://docs.glaredb.com">Documentation</a> •
<a href="https://console.glaredb.com>">GlareDB Cloud</a> •
<a href="https://glaredb.com/blog">Blog</a>
</p>

<div align="center">
<a href="https://github.com/GlareDB/glaredb/releases"><img src="https://img.shields.io/github/v/release/glaredb/glaredb?display_name=tag&style=flat-square"></img></a>
<a href="https://pypi.org/project/glaredb"><img src="https://img.shields.io/pypi/v/glaredb?style=flat-square"</img></a>
<a href="https://twitter.com/glaredb"><img src="https://img.shields.io/twitter/follow/glaredb?color=blue&logo=twitter&style=flat-square"></img></a>
<a href="https://discord.gg/2D7qxC5xkf"><img src="https://img.shields.io/static/v1?label=Chat on Discord&message= &color=360066&style=flat-square"></img></a>
</div>

## Install

Install/update `glaredb` in the **current directory**:

```shell
curl https://glaredb.com/install.sh | sh
```

> [!TIP]
> It may be helpful to install the binary in a location on your `PATH`. For example, `~/.local/bin`.
>
> If you prefer manual installation, download, extract and run the GlareDB binary
> from a release in our [releases page].


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
   glaredb.sql("select 'hello world';").show()
   ```

To use **Hybrid Execution**, sign up at <https://console.glaredb.com> and
use the connection string for your deployment. For example:

```python
import glaredb
con = glaredb.connect("glaredb://user:pass@host:port/deployment")
con.sql("select 'hello hybrid exec';").show()
```

GlareDB also work with [Pandas] and [Polars] DataFrames out of the box:

```py
glaredb.sql("select * from pd_df").to_pandas()
glaredb.sql("select * from pl_df").to_polars()
```

### Local server

```shell
# The `server` subcommand can be used to launch a server process for GlareDB:
./glaredb server
```

> [!NOTE]
> When launched as a server process, GlareDB can be reached on port `6543` using a
> Postgres client. The following example uses `psql` to connect to a locally
> running serve:


```shell
# Connect to the server using psql client
psql "host=localhost user=glaredb dbname=glaredb port=6543"
```

## Building from source

Requirements

- [rust/cargo](https://rustup.rs/)
- [just](https://just.systems)

```shell
git clone https://github.com/GlareDB/glaredb
cd glaredb
just build --release
```

The compiled release binary can be found in `target/release/glaredb`.

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
