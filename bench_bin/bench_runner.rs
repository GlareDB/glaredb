use std::path::{Path, PathBuf};

use ext_csv::extension::CsvExtension;
use ext_iceberg::extension::IcebergExtension;
use ext_parquet::extension::ParquetExtension;
use ext_spark::SparkExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_bench::{RunArgs, RunConfig, TsvWriter, pagecache};
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};

pub fn main() -> Result<()> {
    let run_args = RunArgs {
        print_explain: false,
        print_profile_data: false,
        print_results: false,
        count: 3,
    };

    let writer = TsvWriter::try_new(Some("./results.tsv".into()))?;
    writer.write_header()?;

    run_with_setup::<DefaultSetup>(writer.clone(), run_args, "../bench/micro", false)?;

    writer.flush()?;

    Ok(())
}

trait EngineSetup<E, R>: Sync + Send
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>>;
}

/// Default setup that registers all extensions equivalent to our release
/// binaries.
#[derive(Debug, Clone, Copy)]
struct DefaultSetup;

impl<E, R> EngineSetup<E, R> for DefaultSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        engine.register_extension(SparkExtension)?;
        engine.register_extension(TpchGenExtension)?;
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;
        engine.register_extension(IcebergExtension)?;

        Ok(engine)
    }
}

fn run_with_setup<S>(
    writer: TsvWriter,
    run_args: RunArgs,
    path: &str,
    drop_page_cache: bool,
) -> Result<()>
where
    S: EngineSetup<ThreadedNativeExecutor, NativeSystemRuntime>,
{
    let paths = glaredb_bench::find_files(Path::new(path)).unwrap();

    glaredb_bench::run(writer, run_args, paths, move || {
        if drop_page_cache {
            pagecache::drop_page_cache()?;
        }

        let tokio_rt = new_tokio_runtime_for_io()?;

        let executor = ThreadedNativeExecutor::try_new().unwrap();
        let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

        let engine = SingleUserEngine::try_new(executor.clone(), runtime.clone())?;
        let session = S::setup(engine)?;

        Ok(RunConfig { session, tokio_rt })
    })
}
