use std::path::Path;

use ext_csv::extension::CsvExtension;
use ext_iceberg::extension::IcebergExtension;
use ext_parquet::extension::ParquetExtension;
use ext_spark::SparkExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_bench::{BenchArguments, RunConfig, TsvWriter};
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_core::util::future::block_on;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use harness::Arguments;
use harness::sqlfile::find::find_files;

pub fn main() -> Result<()> {
    let args = Arguments::<BenchArguments>::from_args();

    // Micro benches.
    run_with_setup::<DefaultSetup>(&args, "../bench/micro", "micro")?;

    // Clickbench with a single hits file.
    run_with_setup::<ClickbenchSingleSetup>(
        &args,
        "../bench/clickbench/single",
        "clickbench-parquet-single",
    )?;

    Ok(())
}

trait EngineSetup<E, R>: Sync + Send
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>>;
}

// TODO: Move this to an 'ext_default' crate then shared with benches, cli,
// wasm, python.
fn register_default_extensions<E, R>(engine: &SingleUserEngine<E, R>) -> Result<()>
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

fn run_setup_query<E, R>(engine: &SingleUserEngine<E, R>, query: &str) -> Result<()>
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    block_on(async {
        let mut q_res = engine.session().query(query).await?;
        let _ = q_res.output.collect().await?;
        Ok(())
    })
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
        register_default_extensions(&engine)?;
        Ok(engine)
    }
}

#[derive(Debug, Clone, Copy)]
struct ClickbenchSingleSetup;

impl<E, R> EngineSetup<E, R> for ClickbenchSingleSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        register_default_extensions(&engine)?;

        run_setup_query(
            &engine,
            "
            CREATE TEMP VIEW hits AS
              SELECT * REPLACE (EventDate::DATE AS EventDate)
                FROM read_parquet('../bench/data/clickbench/hits.parquet')
            ",
        )?;

        Ok(engine)
    }
}

fn run_with_setup<S>(args: &Arguments<BenchArguments>, path: &str, tag: &str) -> Result<()>
where
    S: EngineSetup<ThreadedNativeExecutor, NativeSystemRuntime>,
{
    let mut paths = find_files(Path::new(path), ".bench").unwrap();
    paths.sort_unstable();

    // TODO: Weird but whatever.
    let writer = if paths.is_empty() {
        TsvWriter::try_new(None)?
    } else {
        let path = format!("../bench/results-{tag}.tsv");
        TsvWriter::try_new(Some(path.into()))?
    };
    writer.write_header()?;

    glaredb_bench::run(
        writer.clone(),
        args,
        paths,
        || {
            let tokio_rt = new_tokio_runtime_for_io()?;

            let executor = ThreadedNativeExecutor::try_new().unwrap();
            let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

            let engine = SingleUserEngine::try_new(executor.clone(), runtime.clone())?;
            let session = S::setup(engine)?;

            Ok(RunConfig { session, tokio_rt })
        },
        tag,
    )?;

    writer.flush()?;

    Ok(())
}
