use std::path::Path;
use std::time::Duration;

use glaredb_error::Result;
use glaredb_core::datasource::{DataSourceBuilder, DataSourceRegistry};
use rayexec_parquet::ParquetDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::{SingleUserEngine, SingleUserSession};
use rayexec_slt::{ReplacementVars, RunConfig};

pub fn main() -> Result<()> {
    let rt = NativeRuntime::with_default_tokio()?;
    let executor = ThreadedNativeExecutor::try_new()?;

    let paths = rayexec_slt::find_files(Path::new("../slt/tpcds")).unwrap();
    rayexec_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();
            async move {
                let engine = SingleUserEngine::try_new(
                    executor.clone(),
                    rt.clone(),
                    DataSourceRegistry::default()
                        .with_datasource("parquet", ParquetDataSource::initialize(rt.clone()))?,
                )?;

                create_views(engine.session()).await?;

                Ok(RunConfig {
                    engine,
                    vars: ReplacementVars::default(),
                    create_slt_tmp: false,
                    query_timeout: Duration::from_secs(30), // Since these are all running in debug mode.
                })
            }
        },
        "slt_tpcds",
    )
}

async fn create_views(
    session: &SingleUserSession<ThreadedNativeExecutor, NativeRuntime>,
) -> Result<()> {
    const TABLES: &[&str] = &[
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    ];

    for table in TABLES {
        let query = format_view_query(table);
        // println!("{query}");
        let _ = session.query(&query).await?.collect().await?;
    }

    Ok(())
}

fn format_view_query(table: &str) -> String {
    format!("CREATE TEMP VIEW {table} AS SELECT * FROM '../submodules/testdata/tpcds_sf0.1/{table}.parquet';")
}
