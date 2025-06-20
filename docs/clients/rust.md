---
title: Rust
order: 10
---

# Rust API

The GlareDB Rust API is provided through a collection of library crates. These
crates expose core APIs and types allowing for deep integration into existing
Rust programs.

> The Rust API is **unstable** and will continue to evolve.

## Crate overview

- [`glaredb_core`](https://crates.io/crates/glaredb_core): Core query engine
  types and functionality.
- [`glaredb_rt_native`](https://crates.io/crates/glaredb_rt_native): The
  "native" runtime for executing queries and accessing external resources.
- [`glaredb_ext_default`](https://crates.io/crates/glaredb_ext_default): A set
  of default extensions, including extensions for reading Parquet, CSV, and
  more. This is the set of extensions that are shipped in release binaries other
  clients.

## Example usage

Add both `glaredb_core` and `glaredb_rt_native` as dependencies to your Rust
project:

```shell
cargo add glaredb_core
cargo add glaredb_rt_native
```

Initializing a GlareDB engine requires both a **Pipeline Executor** and a
**System Runtime**. The `glaredb_rt_native` crate provides implementations of
both which we can use to create a single user engine:

```rust
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};

let executor = ThreadedNativeExecutor::try_new().unwrap();
let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());
```

Note that `NativeSystemRuntime` requires a Tokio runtime handle, which is used
when executing remote IO. Both "io" and "time" should be enabled for the Tokio
runtime used.

We can now create an engine using the executor and system runtime we just
created.

```rust
use glaredb_core::engine::single_user::SingleUserEngine;

let single_user_engine = SingleUserEngine::try_new(executor, runtime).unwrap();
```

Once we have our engine, we can now register extensions. This example uses
`glaredb_ext_default` to register all default extensions. Alternatively,
individual extensions can be registered if not all extensions are required.

```rust
glaredb_ext_default::register_all(&single_user_engine.engine)?;
```

Now that we have our extensions registered, we can begin querying:

```rust
let mut query_result = engine
    .session()
    .query("SELECT avg(salary), count(*) FROM 's3://glaredb-public/userdata0.parquet'")
    .await
    .unwrap();

let batches = query_result.output.collect().await.unwrap();
```

Note that these methods are async, and are required to be executed on an async
runtime (Tokio) or a utility like `block_on` is used.

Now lets print out the results:

```rust
use glaredb_core::arrays::format::pretty::{components::PRETTY_COMPONENTS, table::PrettyTable};

let table = PrettyTable::try_new(
    &query_result.output_schema,
    &batches,
    100, // Max table width.
    None, // Max number of rows.
    PRETTY_COMPONENTS,
).unwrap();
println!("{table}");
```

See [Examples](https://github.com/GlareDB/glaredb/tree/main/examples) for
end-to-end Rust examples.
