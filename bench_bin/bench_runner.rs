use std::path::{Path, PathBuf};

use ext_csv::extension::CsvExtension;
use ext_iceberg::extension::IcebergExtension;
use ext_parquet::extension::ParquetExtension;
use ext_spark::SparkExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_bench::{BenchArguments, RunConfig};
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

    // TPCH SF=1
    run_with_setup::<TpchSetup<1>>(&args, "../bench/tpch/1", "tpch-parquet-sf-1")?;

    // Clickbench with a single hits file.
    run_with_setup::<ClickbenchSingleSetup>(
        &args,
        "../bench/clickbench/single",
        "clickbench-parquet-single",
    )?;

    run_with_setup::<ClickbenchPartitionedSetup>(
        &args,
        "../bench/clickbench/partitioned",
        "clickbench-parquet-partitioned",
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
struct TpchSetup<const SF: usize>; // sure

impl<E, R, const SF: usize> EngineSetup<E, R> for TpchSetup<SF>
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(engine: SingleUserEngine<E, R>) -> Result<SingleUserEngine<E, R>> {
        register_default_extensions(&engine)?;

        let tables = [
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];

        for table in tables {
            let query = format!(
                "CREATE TEMP VIEW {table}
                   AS SELECT *
                   FROM read_parquet('../bench/data/tpch/{SF}/{table}.parquet')"
            );
            run_setup_query(&engine, &query)?;
        }

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

#[derive(Debug, Clone, Copy)]
struct ClickbenchPartitionedSetup;

impl<E, R> EngineSetup<E, R> for ClickbenchPartitionedSetup
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
              SELECT * REPLACE (
                       EventDate::DATE            AS  EventDate,
                       Title::TEXT                AS  Title,
                       URL::TEXT                  AS  URL,
                       Referer::TEXT              AS  Referer,
                       FlashMinor2::TEXT          AS  FlashMinor2,
                       UserAgentMinor::TEXT       AS  UserAgentMinor,
                       MobilePhoneModel::TEXT     AS  MobilePhoneModel,
                       Params::TEXT               AS  Params,
                       SearchPhrase::TEXT         AS  SearchPhrase,
                       PageCharset::TEXT          AS  PageCharset,
                       OriginalURL::TEXT          AS  OriginalURL,
                       HitColor::TEXT             AS  HitColor,
                       BrowserLanguage::TEXT      AS  BrowserLanguage,
                       BrowserCountry::TEXT       AS  BrowserCountry,
                       SocialNetwork::TEXT        AS  SocialNetwork,
                       SocialAction::TEXT         AS  SocialAction,
                       SocialSourcePage::TEXT     AS  SocialSourcePage,
                       ParamOrderID::TEXT         AS  ParamOrderID,
                       ParamCurrency::TEXT        AS  ParamCurrency,
                       OpenstatServiceName::TEXT  AS  OpenstatServiceName,
                       OpenstatCampaignID::TEXT   AS  OpenstatCampaignID,
                       OpenstatAdID::TEXT         AS  OpenstatAdID,
                       OpenstatSourceID::TEXT     AS  OpenstatSourceID,
                       UTMSource::TEXT            AS  UTMSource,
                       UTMMedium::TEXT            AS  UTMMedium,
                       UTMCampaign::TEXT          AS  UTMCampaign,
                       UTMContent::TEXT           AS  UTMContent,
                       UTMTerm::TEXT              AS  UTMTerm,
                       FromTag::TEXT              AS  FromTag
                       )
                FROM read_parquet('../bench/data/clickbench/partitioned/hits_*.parquet')

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

    let tsv_results_path = PathBuf::from(format!("../bench/results-{tag}.tsv"));

    glaredb_bench::run(
        args,
        paths,
        || {
            let tokio_rt = new_tokio_runtime_for_io()?;

            let executor = ThreadedNativeExecutor::try_new()?;
            let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

            let engine = SingleUserEngine::try_new(executor.clone(), runtime.clone())?;
            let session = S::setup(engine)?;

            Ok(RunConfig { session, tokio_rt })
        },
        tag,
        &tsv_results_path,
    )?;

    Ok(())
}
