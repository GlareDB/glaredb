# Contributing to GlareDB

All commits to `main` should first go through a PR. All CI checks should pass
before merging in a PR. Since we squash merge, PR titles should follow
[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

## Development environment

Developing GlareDB requires that you have Rust and Cargo installed, along with
some additional system dependencies:

- [Protobuf](https://grpc.io/docs/protoc-installation/)
- [Just](https://just.systems/man/en/chapter_1.html)

### Additional tooling

#### Docker

While not strictly required for development, having Docker installed and running
can be helpful in certain scenarios. For example, spinning up a scratch Postgres
instance in Docker can be done via:

```shell
docker run --rm --name my_scratch_postgres -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres:14
```

See [Get Docker](https://docs.docker.com/get-docker/) for info on installing Docker (Desktop).

## Platform support

GlareDB aims to support the following platforms:

- Windows (x86_64)
- MacOS (x86_64 and Arm)
- Linux (x86_64)

Platform specific code should be kept to minimum. At the time of this writing,
the only divergence is with SSH tunnels. The Linux and Mac code paths both use
openssl, while the Windows code path is a stub that returns an "unsupported"
error.

---

## Testing

### Unit Tests

Unit tests attempt to test small parts of the system. These can be ran via
cargo:

```shell
just unit-tests
```

When writing unit tests, aims to keep the scope small with a minimal amount of
setup required.

### Functional Tests

Functional tests are tests executed against running GlareDB instances, and can
provide insight as to whether the system as a whole is working as intended.

There are two types of functional tests in this repo: **SQL Logic Tests** and
**Postgres Protocol Tests**.

### Python (pytest) Integration Tests

The `tests` directory contains a pytest environment useful for
creating black box tests and ecosystem integration tests, and
providing a low-friction way to exercise parts of the codebase that
are difficult to interact with in any other way. To use these tests,
you must:

- build `glaredb` using `just build` or `cargo build --bin glaredb`

- run `just pytest-setup` to configure a virtual environment and
  install dependencies.

Then run the tests, using:

```shell
just pytest
```

#### SQL Logic Tests

SQL logic tests run end-to-end tests that execute actual SQL queries against a
running database. These should be used when testing the logical output of SQL
commands (e.g. asserting that a builtin function returns the correct results, or
ensuring that transactions are appropriately isolated).

You can simply run the binary to run all the tests. Test cases can be found in
`testdata/sqllogictest*`.

```shell
just slt
```

You might have to set a few environment variables for running tests in
`testdata/sqllogictests_*`. These are datasource tests. See [Test Environment
Variables](#test-environment-variables) section for details.

To run basic sqllogictests:

```shell
just slt 'sqllogictests/*'
```

This will run all tests in `testdata/sqllogictests` directory. Basically to run
specific tests you can provide an glob-like regex argument:

```shell
# Run all the tests prefixed with `sqllogictests/cast/`. These are all the tests
# in `testdata/sqllogictest/cast` directory.
#
# Note the quotes (') around `sqllogictests/cast/*`. This is so the shell
# doesn't try and expand the argument into files.
just slt 'sqllogictests/cast/*'
```

Note that, all the test names don't have `.slt` but the runner only picks up
files that have this extension. So, to run the test `testdata/sqllogictests/
simple.slt`, you would run:

```shell
just slt sqllogictests/simple
```

To list the test cases, use the `--list` flag. This flag can be used to check
dry run of all the tests that will run. You can pass the regex along with the
flag as well.

```shell
just slt --list '*/full_outer/*'
```

`sqllogictests` can run either against an external database using the
`--connection-string` flag, or spin up an embedded database by default.

An example invocation using an embedded database:

```shell
just slt --keep-running
```

The `--keep-running` flag will keep the GlareDB server up to allow for
additional debugging. `sqllogictests` will print out the postgres connection
string corresponding to the errored test. You can then connect to it via `psql`,
for example:

```shell
# Logs:
#
# connect to the database using connection string:
#  "host=localhost port=50383 dbname=a2216761-7e80-4156-919f-7c5d56262bac user=glaredb password=glaredb"
#
# Connect to the database using:
psql "host=localhost port=50383 dbname=a2216761-7e80-4156-919f-7c5d56262bac user=glaredb password=glaredb"
```

---

<details>
<summary><h3>Testing large datasets</h3></summary>
<br>

Some testdata is too large to be checked into the repository. For these datasets, we keep them in a public gcs bucket. To pull them run:

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

</details>

## Integration Testing

Many of the sql logic tests require integrating with external systems. As such, you will need to have access to the systems being tested against. We use environment variables to connect to these external systems.

### Test Environment variables

Some SQL Logic Tests depend on setting a few environment variables:

1. **`POSTGRES_CONN_STRING`**: To run postgres datasource tests. Use the string
   returned from setting up the local database (first line):

   ```sh
   POSTGRES_TEST_DB=$(./scripts/create-test-postgres-db.sh)
   export POSTGRES_CONN_STRING=$(echo "$POSTGRES_TEST_DB" | sed -n 1p)
   ```

1. **`POSTGRES_TUNNEL_SSH_CONN_STRING`**: To run postgres datasource tests
   with SSH tunnel. Use the string returned from setting up the local database
   (second line):

   ```sh
   POSTGRES_TEST_DB=$(./scripts/create-test-postgres-db.sh)
   export POSTGRES_TUNNEL_SSH_CONN_STRING=$(echo "$POSTGRES_TEST_DB" | sed -n 2p)
   ```

1. **`MYSQL_CONN_STRING`**: To run the mysql datasource tests. Use the string
   returned from setting up the local database (first line):

   ```sh
   MYSQL_TEST_DB=$(./scripts/create-test-mysql-db.sh)
   export MYSQL_CONN_STRING=$(echo "$MYSQL_TEST_DB" | sed -n 1p)
   ```

1. **`MYSQL_TUNNEL_SSH_CONN_STRING`**: To run the mysql datasource tests with
   SSH tunnel. Use the string returned from setting up the local database
   (second line):

   ```sh
   MYSQL_TEST_DB=$(./scripts/create-test-mysql-db.sh)
   export MYSQL_TUNNEL_SSH_CONN_STRING=$(echo "$MYSQL_TEST_DB" | sed -n 2p)
   ```

1. **`GCP_PROJECT_ID`**: To run the bigquery and GCS tests. For development
   set it to `glaredb-dev-playground`. A custom dataset will be created as a
   part of this project.

   ```sh
   export GCP_PROJECT_ID=glaredb-dev-playground
   ```

1. **`GCP_SERVICE_ACCOUNT_KEY`**: To run the bigquery and GCS tests. Download
   the JSON service account key from cloud dashboard and set the environment
   variable to the contents of the file.

   ```sh
   export GCP_SERVICE_ACCOUNT_KEY=<SERVICE_ACCOUNT_KEY>
   ```

1. **`BIGQUERY_DATASET_ID`**: To run the bigquery tests. Use the string
   returned from setting up a custom dataset in `glaredb-dev-playground`.

   ```sh
   export BIGQUERY_DATASET_ID=$(./scripts/create-test-bigquery-db.sh)
   ```

1. **`SNOWFLAKE_DATABASE`**: To run the snowflake tests. Use the string returned
   from setting up a custom database in the snowflake account (`hmpfscx-
xo23956`).

   ```sh
   export SNOWFLAKE_DATABASE=$(./scripts/create-test-snowflake-db.sh)
   ```

1. **`SNOWFLAKE_USERNAME`**: To run the snowflake tests. Your snowflake
   username.

   ```sh
   export SNOWFLAKE_USERNAME=<USERNAME>
   ```

1. **`SNOWFLAKE_PASSWORD`**: To run the snowflake tests. Set it to the password
   corresponding to the _SNOWFLAKE_USERNAME_.

   ```sh
   export SNOWFLAKE_PASSWORD=...
   ```

1. **`MONGO_CONN_STRING`**: To run the mongodb tests. Use the string returned
   from setting up the local database:

   ```sh
   export MONGO_CONN_STRING=$(./scripts/create-test-mongo-db.sh)
   ```

##### Writing Tests

Each test file should start with a short comment describing what the file is
testing, as well as set up a unique schema to work within. E.g.
`join_on_aggregates.slt` should have something like the following at the top of
the file:

```text
# Test join on aggregates

statement ok
create schema join_on_aggregates;

statement ok
set search_path = join_on_aggregates;
```

Creating a schema and setting the search path to that schema provides isolation
between tests without having to fully qualify table names and other resources.

An example SQL logic test is as follows:

```text
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

###### Environment variables

In addition to the above mentioned document, we run a modified version of the
SQL Logic Tests runner. We support environment variables which can be used
anywhere in the script using the notation: `${ MY_ENV_VARIABLE }`.

For example:

```
statement ok
select * from ${MY_TABLE_NAME};
```

Setting the environment variable `export MY_TABLE_NAME=cool_table`, the script
will be translated to:

```
statement ok
select * from cool_table;
```

##### Interpreting Test Output

`sqllogictests` stops at the first error encountered.

GlareDB logging and `sqllogictests` output is currently intermingled.
`sqllogictests` prints everything unadorned, so output will not be prefixed with
any logging metadata like timestamps or thread IDs.

Errors in the expected output of a query will print a color-coded diff between
the expected and actual results:

```text
2023-05-19T07:24:58.857496Z ERROR main ThreadId(01) testing::slt::cli: crates/testing/src/slt/cli.rs:209: Error while running test `simple` error=test fail: query result mismatch:
[SQL] select * from (values (1, 2, 3), (3, 4, 5))
[Diff]
1 2 4 <-- RED
1 2 3 <-- GREEN
3 4 5

at /path/to/testdata/sqllogictests/simple.slt:13

keeping the server running, addr: [::1]:42219
CTRL-C to exit
```

Other errors not related to comparing expected and actual output (e.g. failing
to parse, missing function) will look something like the following:

```text
2023-05-19T07:24:58.857496Z ERROR main ThreadId(01) testing::slt::cli: crates/testing/src/slt/cli.rs:209: Error while running test `simple` error=test fail: statement failed: db error: ERROR: failed to execute: DataFusion(SchemaError(FieldNotFound { qualifier: None, name: "function_does_not_exist", valid_fields: Some([]) }))
[SQL] select function_does_not_exist;
at testdata/sqllogictests/simple.slt:19

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

Test cases can be found in `./testdata/pgprototest` and
`./testdata/pgprototest_glaredb`.

The `pgprototest` directory is for test cases to assert that GlareDB matches
Postgres exactly, and the expected output should be generated from an actual
Postgres instance.

The `pgprototest_glaredb` directory contains test cases that do match Postgres
output exactly either because of an incomplete feature, or differing behavior.
The expected output for these tests need to be hand-crafted.

Tests can be ran with the `pgprototest` command:

```shell
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

```text
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

```text
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

```shell
docker run --rm --name "my_postgres" -p 5432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust -d postgres:14
```

Now run `pgprototest` with rewriting:

```shell
cargo run -p pgprototest -- --dir ./testdata/pgprototest --addr localhost:5432 --user postgres --database postgres --rewrite
```

If everything runs correctly, you should see the `until` that we wrote above
gets rewritten to the following:

```text
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

```shell
cargo run -p pgprototest -- --dir ./testdata/pgprototest --addr localhost:6543 --user glaredb --password dummy --database glaredb
```

If all goes well, we exit with a 0 status code, indicating that we return the
same messages as Postgres.

If there's a mismatch, `pgpprototest` will print out the expected and actual
results for the failing test, e.g.:

```text
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

[^1]:
    `pgprototest` onlys support clear text authentication. Postgres will
    typically use SASL when doing password based authentication, so it's
    recommended to configure Postgres with no password to avoid this shortcoming
    (e.g. via the `POSTGRES_HOST_AUTH_METHOD=true` environment variable for the
    docker container).
