use ext_csv::extension::CsvExtension;
use ext_iceberg::extension::IcebergExtension;
use ext_parquet::extension::ParquetExtension;
use ext_spark::SparkExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_core::engine::Engine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::Result;

/// Registers all default extensions with the given engine.
pub fn register_all<E, R>(engine: &Engine<E, R>) -> Result<()>
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    engine.register_extension(SparkExtension)?;
    engine.register_extension(TpchGenExtension)?;
    engine.register_extension(CsvExtension)?;
    engine.register_extension(ParquetExtension)?;
    engine.register_extension(IcebergExtension)?;

    Ok(())
}
