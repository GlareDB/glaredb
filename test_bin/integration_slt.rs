use std::path::Path;
use std::time::Duration;

use ext_csv::extension::CsvExtension;
use ext_parquet::extension::ParquetExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeExecutor,
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use glaredb_rt_native::threaded::ThreadedScheduler;
use glaredb_slt::{ReplacementVars, RunConfig};

pub fn main() -> Result<()> {
    // Standard tests.
    run_with_all_thread_configurations::<StandardSetup>("../slt/standard", "slt_standard")?;

    // TPC-H gen extension.
    run_with_all_thread_configurations::<TpchGenSetup>("../slt/tpch_gen", "slt_tpch_gen")?;

    // CSV extension.
    run_with_all_thread_configurations::<CsvSetup>("../slt/csv", "slt_csv")?;

    // Parquet extension.
    run_with_all_thread_configurations::<ParquetSetup>("../slt/parquet", "slt_parquet")?;

    Ok(())
}

trait EngineSetup<E, R>: Sync + Send
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>>;
}

#[derive(Debug, Clone, Copy)]
struct StandardSetup;

impl<E, R> EngineSetup<E, R> for StandardSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        Ok(engine)
    }
}

#[derive(Debug, Clone, Copy)]
struct TpchGenSetup;

impl<E, R> EngineSetup<E, R> for TpchGenSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        engine.register_extension(TpchGenExtension)?;
        Ok(engine)
    }
}

#[derive(Debug, Clone, Copy)]
struct CsvSetup;

impl<E, R> EngineSetup<E, R> for CsvSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        engine.register_extension(CsvExtension)?;
        Ok(engine)
    }
}

#[derive(Debug, Clone, Copy)]
struct ParquetSetup;

impl<E, R> EngineSetup<E, R> for ParquetSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        engine.register_extension(ParquetExtension)?;
        Ok(engine)
    }
}

fn run_with_executor<S>(executor: ThreadedNativeExecutor, path: &str, tag: &str) -> Result<()>
where
    S: EngineSetup<NativeExecutor<ThreadedScheduler>, NativeSystemRuntime>,
{
    let tokio_rt = new_tokio_runtime_for_io()?;
    let rt = NativeSystemRuntime::new(tokio_rt.handle().clone());

    let paths = glaredb_slt::find_files(Path::new(path)).unwrap();
    glaredb_slt::run(
        paths,
        move || {
            let executor = executor.clone();
            let rt = rt.clone();

            async move {
                let engine = SingleUserEngine::try_new(executor.clone(), rt.clone())?;
                let engine = S::setup(engine)?;

                Ok(RunConfig {
                    engine,
                    vars: ReplacementVars::default(),
                    create_slt_tmp: false,
                    query_timeout: Duration::from_secs(5),
                })
            }
        },
        tag,
    )
}

fn run_with_all_thread_configurations<S>(path: &str, tag: &str) -> Result<()>
where
    S: EngineSetup<NativeExecutor<ThreadedScheduler>, NativeSystemRuntime>,
{
    // Executor with a the default number of threads (auto-detected).
    run_with_executor::<S>(
        ThreadedNativeExecutor::try_new()?,
        path,
        &format!("{}/default", tag),
    )?;

    // Executor using a single thread.
    run_with_executor::<S>(
        ThreadedNativeExecutor::try_new_with_num_threads(1)?,
        path,
        &format!("{}/single", tag),
    )?;

    // Executor using a hardcode number of threads.
    run_with_executor::<S>(
        ThreadedNativeExecutor::try_new_with_num_threads(16)?,
        path,
        &format!("{}/multi", tag),
    )?;

    Ok(())
}
