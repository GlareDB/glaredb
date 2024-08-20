use rayexec_bullet::{
    array::{Array, PrimitiveArray, VarlenArray},
    batch::Batch,
    datatype::DataType,
    field::Field,
};
use rayexec_debug::{table_storage::TablePreload, DebugDataSource, DebugDataSourceOptions};
use rayexec_error::Result;
use rayexec_execution::{
    datasource::DataSourceRegistry,
    engine::Engine,
    hybrid::client::{HybridClient, HybridConnectConfig},
    runtime::{Runtime, TokioHandlerProvider},
};
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_server::serve_with_engine;
use rayexec_slt::{ReplacementVars, RunConfig};
use std::{collections::HashMap, path::Path, sync::Arc};

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
                    data: Batch::try_new([
                        Array::Int64(PrimitiveArray::from_iter([1, 2])),
                        Array::Utf8(VarlenArray::from_iter(["a", "b"])),
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
                    data: Batch::try_new([
                        Array::Int64(PrimitiveArray::from_iter([1, 2])),
                        Array::Utf8(VarlenArray::from_iter(["a", "b"])),
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

    // Client engine.
    let rt = NativeRuntime::with_default_tokio()?;
    let engine = Arc::new(Engine::new_with_registry(
        ThreadedNativeExecutor::try_new()?,
        rt.clone(),
        DataSourceRegistry::default().with_datasource(
            "local_debug1",
            Box::new(DebugDataSource::new(DebugDataSourceOptions {
                preloads: Vec::new(),
                expected_options: HashMap::new(),
                discard_format: "discard_local".to_string(),
            })),
        )?,
    )?);

    let paths = rayexec_slt::find_files(Path::new("../slt/hybrid")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let mut session = engine.new_session()?;

            // TODO: This is duplicated with `connect_hybrid` in `rayexec_shell`.

            let connection_string = format!("http://localhost:{}", PORT);
            let config = HybridConnectConfig::try_from_connection_string(&connection_string)?;
            let client = rt.http_client();
            let hybrid = HybridClient::new(client, config);

            session.set_hybrid(hybrid);

            Ok(RunConfig {
                session,
                vars: ReplacementVars::default(),
                create_slt_tmp: false,
            })
        },
        "slt_hybrid",
    )
}
