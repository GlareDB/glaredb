# Contributing to GlareDB

All commits to `main` should first go through a PR. All CI checks should pass
before merging in a PR. Since we squash merge, PR titles should follow
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

Development dependencies may (optionally) be installed using nix. By using nix
all programs needed (cargo, protoc, &c) will be installed and you will be placed
into a shell configured with them in your PATH.

## Nix

### Obtaining a development environment

First, if you are not using NixOS you must install nix and configure the
experimental flakes feature.

The [nix download page](https://nixos.org/download.html) has instructions on installation. To enable flakes,
add the following to your `nix.conf` in either `~/.config/nix/nix.conf` or
`/etc/nix/nix.conf`

```
experimental-features = nix-command flakes
```

The nixos wiki has [instructions for enabling flakes](https://nixos.wiki/wiki/Flakes#Enable_flakes) which should have the
most up to date information.

When all of these steps are complete, you should be able to obtain the projects
development shell: `nix develop` From this shell you may work on the project and
use any cli tools needed.

### other information

This project uses [flake-parts](https://github.com/hercules-ci/flake-parts) to organize the nix flake output into
multiple files. `flake.nix` is kept minimal and the [flake outputs](https://nixos.wiki/wiki/Flakes#Output_schema) are
gathered from subdirectories that share their names inside the `flake-parts`
directory. Insight into each of these may be gained by looking at the nix source
and/or their READMEs.

### nix commands

| command          | description                                                                                        | relevant flake part path                     |
|------------------|----------------------------------------------------------------------------------------------------|----------------------------------------------|
| `nix develop`    | obtain development shell                                                                           | [flake-parts/devshell](flake-parts/devshell) |
| `nix build`      | build packages (binaries, container images, &c)                                                    | [flake-parts/packages](flake-parts/packages) |
| `nix flake show` | print all outputs provided by the flake. useful since the project is organized into multiple files | N/A                                          |

### direnv

[nix-direnv](https://github.com/nix-community/nix-direnv) works especially well with nix devshells. If you install it you
may place a `.envrc` in your local workspace with the contents: `use flake` and
the project's devshell will automatically be entered when inside its directory.

## Testing

### Unit Tests

Unit tests attempt to test small parts of the system. These can be ran via
cargo:

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
cargo run --bin slt_runner -- --keep-running testdata/sqllogictests/**/*.slt
```

The `--keep-running` flag will keep the GlareDB server up to allow for
additional debugging. `slt_runner` will print out the address that the GlareDB
is running on. You can then connect to it via `psql`, for example:

``` shell
psql "host=localhost dbname=glaredb port=<port> user=glaredb"
```

To run using Google Cloud Storage:
- Ensure bucket has been created on GCS
- Give service account `Storage Object Creator` and `Storage Object Viewer`  roles in bucket permissions
``` shell
export GCS_BUCKET_NAME="<bucket name>"
export GCS_SERVICE_ACCOUNT_PATH="<path to service account info>"
cargo run --bin slt_runner -- --object-store gcs --keep-running testdata/sqllogictests/**/*.slt
```

#### Writing Tests

Each test file should start with a short comment describing what the file is
testing, as well as set up a unique schema to work within. E.g.
`join_on_aggregates.slt` should have something like the following at the top of
the file:

``` text
# Test join on aggregates

statement ok
create schema join_on_aggregates;

statement ok
set search_path = join_on_aggregates;
```

Creating a schema and setting the search path to that schema provides isolation
between tests without having to fully qualify table names and other resources.

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

#### Interpreting Test Output

`slt_runner` stops at the first error encountered.

GlareDB logging and `slt_runner` output is currently intermingled. `slt_runner`
prints everything unadorned, so output will not be prefixed with any logging
metadata like timestamps or thread IDs.

Errors in the expected output of a query will print a color-coded diff between
the expected and actual results:

``` text
test fail: test error at testdata/sqllogictests/simple.slt:13: query result mismatch:
[SQL] select * from (values (1, 2, 3), (3, 4, 5))
[Diff]
1 2 4 <-- RED
1 2 3 <-- GREEN
3 4 5

keeping the server running, addr: [::1]:42219
CTRL-C to exit
```

Other errors not related to comparing expected and actual output (e.g. failing
to parse, missing function) will look something like the following:

``` text
test fail: test error at testdata/sqllogictests/simple.slt:19: statement failed: db error: ERROR: failed to execute: DataFusion(SchemaError(FieldNotFound { qualifier: None, name: "function_does_not_exist", valid_fields: Some([]) }))
[SQL] select function_does_not_exist;
keeping the server running, addr: [::1]:36385
CTRL-C to exit
```

GlareDB should almost never close a connection due to encountering an error
running input from a sql logic test. If it does, **it is very likely a bug**
and an issue should be opened. See [Issue #217](https://github.com/GlareDB/glaredb/issues/217).


