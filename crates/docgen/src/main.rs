mod file;
mod markdown_table;
mod section;
mod session;

use file::DocFile;
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceBuilder, DataSourceRegistry, MemoryDataSource};
use rayexec_iceberg::IcebergDataSource;
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use rayexec_shell::session::SingleUserEngine;
use rayexec_unity_catalog::UnityCatalogDataSource;
use section::ScalarFunctionWriter;
use session::DocsSession;
use tracing::info;

const FILES: &[DocFile] = &[DocFile {
    path: "docs/sql/functions.md",
    sections: &[("scalar_functions", &ScalarFunctionWriter)],
}];

fn main() -> Result<()> {
    logutil::configure_global_logger(tracing::Level::INFO, logutil::LogFormat::HumanReadable);

    info!("starting docs gen");

    let executor = ThreadedNativeExecutor::try_new().unwrap();
    let runtime = NativeRuntime::with_default_tokio().unwrap();

    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", PostgresDataSource::initialize(runtime.clone()))?
        .with_datasource("delta", DeltaDataSource::initialize(runtime.clone()))?
        .with_datasource("unity", UnityCatalogDataSource::initialize(runtime.clone()))?
        .with_datasource("parquet", ParquetDataSource::initialize(runtime.clone()))?
        .with_datasource("csv", CsvDataSource::initialize(runtime.clone()))?
        .with_datasource("iceberg", IcebergDataSource::initialize(runtime.clone()))?;
    let engine = SingleUserEngine::try_new(executor, runtime, registry)?;
    let session = DocsSession { engine };

    for file in FILES {
        info!(%file.path, "handing file");
        file.overwrite(&session)?;
    }

    info!("completed all files");

    Ok(())
}
