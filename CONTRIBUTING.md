# Contributing to GlareDB

All commits to `main` should first go through a PR. All CI checks should pass
before merging in a PR. Since we squash merge, PR titles should follow
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

Development dependencies may (optionally) be installed using nix. By using nix
all programs needed (cargo, protoc, &c) will be installed and you will be placed
into a shell configured with them in your PATH.

## Development environemnt

A development environment can either be setup manually or with nix.

### Manual setup

A manual setup requires that you have Rust and Cargo installed, along with some
additional system dependencies:

- OpenSSL dev bindings
- Protobuf (protoc)
- Possibly more

Note that some test scripts that are run in CI will assume the use of nix.

### Nix setup

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

### Additional tooling

#### Docker

While not strictly required for development, having Docker installed and running
can be helpful in certain scenarios. For example, spinning up a scratch Postgres
instance in Docker can be done via:

``` shell
docker run --rm --name my_scratch_postgres -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres:14
```

See [Get Docker](https://docs.docker.com/get-docker/) for info on installing Docker (Desktop).

## Testing

### Unit Tests

Unit tests attempt to test small parts of the system. These can be ran via
cargo:

``` shell
cargo test
```

When writing unit tests, aims to keep the scope small with a minimal amount of
setup required.

### Functional Tests

Functional tests are tests executed against running GlareDB instances, and can
provide insight as to whether the system as a whole is working as intended.

There are two types of functional tests in this repo: **SQL Logic Tests** and
**Postgres Protocol Tests**.

#### SQL Logic Tests

SQL logic tests run end-to-end tests that execute actual SQL queries against a
running database. These should be used when testing the logical output of SQL
commands (e.g. asserting that a builtin function returns the correct results, or
ensuring that transactions are appropriately isolated).

Test cases can be found in `testdata/sqllogictest`.

`slt_runner` can run either against an external database using the `external`
subcommand, or spin up an embedded database with the `embedded` subcommand.

An example invocation using an embedded database:

``` shell
cargo run --bin slt_runner -- embedded --keep-running testdata/sqllogictests/**/*.slt
```

The `--keep-running` flag will keep the GlareDB server up to allow for
additional debugging. `slt_runner` will print out the address that the GlareDB
is running on. You can then connect to it via `psql`, for example:

``` shell
psql "host=localhost dbname=glaredb port=<port> user=glaredb"
```

##### Test data on the cloud

Some testdata lives in the cloud in GCS bucket `glaredb-testdata` (in the
`glaredb-artifacts` project). This test data is mostly too huge to store in the
repository. To pull the data run:

```shell
./scripts/prepare-testdata.sh
```

While developing you might need to add more testdata to the cloud. In order to
do so, get permissions to push to the project, push the testdata and add it to
the script as `<local path>:<object name>`. Object will be downloaded as
`<local path>/gcs-artifacts/<object name>`.

While running the `prepare-testdata.sh` you can specify the index number of the
object you want to download (in case you don't want to download everything).
Make sure to add a comment with index number when adding new objects to script.

```shell
# Downloads the object at index 13 in the array
./scripts/prepare-testdata.sh 13
```

##### Writing Tests

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

##### Interpreting Test Output

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

#### Postgres Protocol Tests

Postgres protocol tests are tests that send raw protocol messages to the server
and asserts that we receive the appropriate backend messages. These tests ensure
postgres protocol compatability as well as allowing us to assert the contents of
error and notice messages.

Test cases can be found in `./testdata/pgprototest`.

Tests can be ran with the `pgprototest` command:

``` shell
cargo run -p pgprototest pgprototest -- --dir ./testdata/pgprototest --addr localhost:6543 --user glaredb --password dummy --database glaredb
```

This expects that there's a running GlareDB instance on port 6543.

##### Writing Tests

Writing a test case is a 4 step process:

1. Write the frontend messages you want to send.
2. Write the type(s) of backend messages that we should wait for indicating that
   the query is complete. In many cases, this will be one or more
   `ReadyForQuery` messages.
3. Execute `pgprototest` against a running Postgres instance with rewriting
   enabled. This will rewrite the file to include the backend messages that we
   receive from Postgres.
4. Execute `pgprototest` against a running GlareDB instance with rewriting
   disabled. A successful run indicates we are returning the same messages as
   Postgres for that test case.
   
Breaking each step down further...

Step 1: Write the frontend messages you want to send. For example, a simple
query:

``` text
send
Query {"query": "select 1, 2"}
----

```

We prepend the message(s) we want to send with the `send` directive. 

The next line contains the frontend message type (`Query`) and the actual
contents of the message as JSON `{"query": "select 1, 2"}`.

Lines immediately following `----` indicate the expected output of each "block".
**Blocks with the `send` will always have empty results.**

Step 2: Write the type(s) of messages you expect to receive from the backend.
For our simple query, once we receive a `ReadyForQuery`, we know that the "flow"
for this query is over:

``` text
until
ReadyForQuery
----

```

The `until` directive tell the test runner to read all backend message up to and
including `ReadyForQuery`.

The expected output is empty. This will be filled in during test rewriting
(next).

Step 3: Execute against a running postgres instance.

For this, ensure you have a Postgres instance running. For example, with docker[^1]:

``` shell
docker run --rm --name "my_postgres" -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres:14
```

Now run `pgprototest` with rewriting:

``` shell
cargo run -p pgprototest pgprototest -- --dir ./testdata/pgprototest --addr localhost:5432 --user postgres --database postgres --rewrite
```

If everything runs correctly, you should see the `until` that we wrote above
gets rewritten to the following:

``` text
until
ReadyForQuery
----
RowDescription {"fields":[{"name":"?column?"},{"name":"?column?"}]}
DataRow {"fields":["1","2"]}
CommandComplete {"tag":"SELECT 1"}
ReadyForQuery {"status":"I"}
```

What's being show here are all the messages that Postgres send back to us for
our simple query.

Step 4: Execute against a running GlareDB instance. This will ensure that the
messages we receive from GlareDB match the messages we received from Postgres in
Step 3.

``` shell
cargo run -p pgprototest pgprototest -- --dir ./testdata/pgprototest --addr localhost:6543 --user glaredb --password dummy --database glaredb
```

If all goes well, we exit with a 0 status code, indicating that we return the
same messages as Postgres.

If there's a mismatch, `pgpprototest` will print out the expected and actual
results for the failing test, e.g.:

``` text
./testdata/pgprototest/simple.pt:7:
ReadyForQuery

expected:
RowDescription {"fields":[{"name":"?column?"},{"name":"?column?"}]}
DataRow {"fields":["1","2"]}
CommandComplete {"tag":"SELECT 1"}
ReadyForQuery {"status":"I"}

actual:
RowDescription {"fields":[{"name":"?column?"},{"name":"?column?"}]}
DataRow {"fields":["1","2"]}
CommandComplete {"tag":"SELECT 2"}
ReadyForQuery {"status":"I"}
```

[^1]: `pgprototest` onlys support clear text authentication. Postgres will
    typically use SASL when doing password based authentication, so it's
    recommended to configure Postgres with no password to avoid this shortcoming
    (e.g. via the `POSTGRES_HOST_AUTH_METHOD=true` environment variable for the
    docker container).
