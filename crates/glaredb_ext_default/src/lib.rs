use glaredb_core::engine::Engine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::Result;
use glaredb_ext_csv::extension::CsvExtension;
use glaredb_ext_iceberg::extension::IcebergExtension;
use glaredb_ext_parquet::extension::ParquetExtension;
use glaredb_ext_spark::SparkExtension;
use glaredb_ext_tpch_gen::TpchGenExtension;
pub use {
    glaredb_ext_csv,
    glaredb_ext_iceberg,
    glaredb_ext_parquet,
    glaredb_ext_spark,
    glaredb_ext_tpch_gen,
};

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
