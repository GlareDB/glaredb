use std::error::Error;

use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};

fn main() -> Result<(), Box<dyn Error>> {
    // Create a tokio runtime that's used for remote io (object storage). If a
    // tokio runtime already exists, this can be skipped. A handle to the
    // existing tokio runtime should be passed to NativeSystemRuntime.
    let tokio_rt = new_tokio_runtime_for_io()?;

    // Create an 'executor' for executing the queries. `try_new` will create a
    // thread pool using all threads. `try_new_with_num_threads` may be used as
    // an alternative.
    let executor = ThreadedNativeExecutor::try_new()?;

    // Create a 'runtime' for accessing external data (e.g. file systems, how
    // http clients are created, etc).
    let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

    // Simple wrapper around a database engine and a single session connected to
    // that db engine.
    let engine = SingleUserEngine::try_new(executor, runtime.clone())?;

    // Register all default extensions (parquet, csv, etc). Alternatively
    // individual `ext_` crates can be used to selectively register extensions.
    glaredb_ext_default::register_all(&engine.engine)?;

    let (batches, schema) = tokio_rt.block_on(async {
        // Begin execution of a single query. `query` expects a single sql
        // statement, `query_many` can be used to execute multiple statements
        // (with a bit more code required to do so).
        let mut q_res = engine
            .session()
            .query("SELECT avg(salary), count(*) FROM 's3://glaredb-public/userdata0.parquet'")
            .await?;

        // Collect all batches. Execution is streaming, `collect` just fetches
        // everything.
        let batches = q_res.output.collect().await?;

        Ok::<_, Box<dyn Error>>((batches, q_res.output_schema))
    })?;

    // Create and display a pretty table containing the output data in batches.
    let table = PrettyTable::try_new(&schema, &batches, 100, None, PRETTY_COMPONENTS)?;
    println!("{table}");

    Ok(())
}
