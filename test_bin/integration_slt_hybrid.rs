use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use rayexec_debug::table_storage::TablePreload;
use rayexec_debug::{DebugDataSource, DebugDataSourceOptions};
use rayexec_error::Result;
use rayexec_execution::arrays::array::Array2;
use rayexec_execution::arrays::batch::Batch2;
use rayexec_execution::arrays::datatype::DataType;
use rayexec_execution::arrays::field::Field;
use rayexec_execution::datasource::DataSourceRegistry;
use rayexec_execution::engine::Engine;
use rayexec_execution::runtime::{Runtime, TokioHandlerProvider};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_server::serve_with_engine;
use rayexec_shell::session::SingleUserEngine;
use rayexec_slt::{ReplacementVars, RunConfig};

pub fn main() -> Result<()> {
    const PORT: u16 = 8085;

    {
        // Server engine.
        let rt = NativeRuntime::with_default_tokio()?;
        let tokio_handle = rt.tokio_handle().handle().expect("tokio to be configured");

        // TODO: Debug data source with configurable tables, table functions,
        // errors, etc.
        let opts = DebugDataSourceOptions {
            preloads: vec![
                TablePreload {
                    schema: "schema1".to_string(),
                    name: "table1".to_string(),
                    columns: vec![
                        Field::new("c1", DataType::Int64, false),
                        Field::new("c2", DataType::Utf8, false),
                    ],
                    data: Batch2::try_new([
                        Array2::from_iter([1_i64, 2_i64]),
                        Array2::from_iter(["a", "b"]),
                    ])?,
                },
                // Table specific to insert into. Don't rely on this outside of
                // the 'insert_into.slt' since it's global to the engine.
                TablePreload {
                    schema: "schema1".to_string(),
                    name: "insert_into1".to_string(),
                    columns: vec![
                        Field::new("c1", DataType::Int64, false),
                        Field::new("c2", DataType::Utf8, false),
                    ],
                    data: Batch2::try_new([
                        Array2::from_iter([1_i64, 2_i64]),
                        Array2::from_iter(["a", "b"]),
                    ])?,
                },
            ],
            expected_options: HashMap::new(),
            discard_format: "discard_remote".to_string(),
        };

        let datasources = DataSourceRegistry::default()
            .with_datasource("remote_debug1", Box::new(DebugDataSource::new(opts)))?;
        let engine =
            Engine::new_with_registry(ThreadedNativeExecutor::try_new()?, rt.clone(), datasources)?;

        tokio_handle.spawn(async move { serve_with_engine(engine, PORT).await });
    }

    let rt = NativeRuntime::with_default_tokio()?;
    let executor = ThreadedNativeExecutor::try_new()?;

    let paths = rayexec_slt::find_files(Path::new("../slt/hybrid")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();
            async move {
                // Client engine.
                let engine = SingleUserEngine::try_new(
                    executor.clone(),
                    rt.clone(),
                    DataSourceRegistry::default().with_datasource(
                        "local_debug1",
                        Box::new(DebugDataSource::new(DebugDataSourceOptions {
                            preloads: Vec::new(),
                            expected_options: HashMap::new(),
                            discard_format: "discard_local".to_string(),
                        })),
                    )?,
                )?;

                let connection_string = format!("http://localhost:{}", PORT);
                engine.connect_hybrid(connection_string).await?;

                Ok(RunConfig {
                    engine,
                    vars: ReplacementVars::default(),
                    create_slt_tmp: false,
                    query_timeout: Duration::from_secs(5),
                })
            }
        },
        "slt_hybrid",
    )
}
