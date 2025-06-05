---
title: CLI
order: 5
---

# CLI

The GlareDB CLI is a standalone binary for executing SQL queries against
supported data sources and catalogs.

See [Install](../get-started/install.md) for instructions on how to install the
CLI.

## Usage

All examples assume `glaredb` is in your PATH. If it is not in your path,
replace all invocations of `glaredb` with the path to the binary.

`glaredb` can be executed as follows:

```shell
glaredb [OPTIONS]
```

Where `OPTIONS` can include:

- `--init`: Execute a file containing SQL statements and continue.
- `--files <FILES>`: Execute a file containing SQL statements then exit.
- `--commands <COMMANDS>`: Execute SQL commands then exit.

To view the full list of options, pass the `--help` flag.

### Interactive shell

When provided with no arguments, or just an `--init` argument, `glaredb` will
run with an interactive shell.

#### Executing SQL

From the shell, you can run SQL commands. For example, we can copy and paste
this command into the shell:

```sql
SELECT avg(salary), count(*) FROM 'gs://glaredb-public/userdata0.parquet';
```

This statement ends with a semicolon (';') so when we press enter, the statement
will execute and display the results.

If a statement does not end with a semicolon, a new line will be inserted. This
allows you write and edit multiline statements.

#### Dot commands

"Dot commands" are input commands that start with a dot ('.') and are used to
modify the behavior the shell.

For example, if we wanted to adjust the number of rows displayed after executing
the query, we can use the `.maxrows` dot command:

```text
.maxrows 10
```

This will limit the results to only display at most 10 rows -- 5 rows from the
start, and 5 rows from the end.

To see a full list of dot commands, run `.help` in the shell.

Note that dot commands do not need to end with a semicolon. Some dot commands
require arguments while others don't. The output `.help` will show if a dot
command requires and argument.

#### Exiting

Exiting the interactive shell can be exited by pressing `ctrl-c` twice.

### Non-interactive usage

The `--files` and `--commands` arguments can be used to facilitate
non-interactive usage. Both flags can be specified more than once, and can be
used with each other.

When provided one or more files with the `--files` (or `-f`) flag, the files are
read in order, with each SQL statement in each file being executed in order. A
file may contain multiple SQL statements separated by a semicolon. If any
statement errors, then GlareDB will exit with an error code and not execute any
remaining statements.

The `--commands` (or `-c`) flag can be used to provide either SQL commands or
dot commands to execute. Commands are are executed in the order in which they're
provided. If any command errors, GlareDB will exit and not execute any of the
remaining commands.

For example, if we wanted to enable timer output for a non-interactive query, we
can pass in the `.timer` dot command before the query:

```shell
glaredb -c '.timer on' -c 'SELECT 3'
```

This will enable the timer before executing the SQL query.

`--files` and `--commands` can be used with each other. All statements from
files will be executed before any command provided by `--commands`, even if
`--commands` is passed in first.

The `--init` flag can be used in conjunction with the above, and will _always_
execute the SQL statements in the init file before processing any other
argument.

