use std::io;
use std::path::Path;
use std::time::Duration;

use ext_csv::extension::CsvExtension;
use ext_iceberg::extension::IcebergExtension;
use ext_parquet::extension::ParquetExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_core::util::future::block_on;
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeExecutor,
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};
use glaredb_rt_native::threaded::ThreadedScheduler;
use glaredb_slt::{RunConfig, SltArguments};
use harness::Arguments;
use harness::sqlfile::find::find_files;
use harness::sqlfile::vars::{ReplacementVars, VarValue};
use tokio::runtime::Runtime as TokioRuntime;

pub fn main() -> Result<()> {
    let args = Arguments::<SltArguments>::from_args();
    logutil::configure_global_logger(
        logutil::tracing::Level::WARN,
        logutil::LogFormat::HumanReadable,
        io::stderr,
    );

    // Standard tests.
    run_with_all_thread_configurations::<StandardSetup>(&args, "../slt/standard", "slt_standard")?;

    // TPC-H gen extension.
    run_with_all_thread_configurations::<TpchGenSetup>(&args, "../slt/tpch_gen", "slt_tpch_gen")?;

    // CSV extension.
    run_with_all_thread_configurations::<CsvSetup>(&args, "../slt/csv", "slt_csv")?;

    // Parquet extension.
    run_with_all_thread_configurations::<ParquetSetup>(&args, "../slt/parquet", "slt_parquet")?;

    // Iceberg extension.
    run_with_all_thread_configurations::<IcebergSetup>(&args, "../slt/iceberg", "slt_iceberg")?;

    // Read files over http
    run_with_all_thread_configurations::<HttpSetup>(&args, "../slt/http", "slt_http")?;

    // Public S3 with CSV, parquet, iceberg
    run_with_all_thread_configurations::<S3PublicSetup>(
        &args,
        "../slt/s3/public",
        "slt_s3_public",
    )?;

    // Private S3 with CSV, parquet, iceberg
    run_with_all_thread_configurations::<S3PrivateSetup>(
        &args,
        "../slt/s3/private",
        "slt_s3_private",
    )?;

    // Public GCS with CSV, parquet, iceberg
    run_with_all_thread_configurations::<GcsPublicSetup>(
        &args,
        "../slt/gcs/public",
        "slt_gcs_public",
    )?;

    // Private GCS with CSV, parquet, iceberg
    run_with_all_thread_configurations::<GcsPrivateSetup>(
        &args,
        "../slt/gcs/private",
        "slt_gcs_private",
    )?;

    // Clickbench queries on a truncated dataset (single parquet file)
    run_with_all_thread_configurations::<ClickbenchSingleSetup>(
        &args,
        "../slt/clickbench/single",
        "slt_clickbench_single",
    )?;

    // Clickbench queries on a truncated dataset (partitioned parquet files)
    //
    // These should run without 'verify_optimized_plan' since the unoptimized
    // plan will end up reading all columns from all files. The total row count
    // is 100,000.
    run_with_all_thread_configurations::<ClickbenchPartitionedSetup>(
        &args,
        "../slt/clickbench/partitioned",
        "slt_clickbench_partitioned",
    )?;

    // TPC-H queries on a SF=0.1 dataset
    //
    // These should all run without 'verify_optimized_plan' as the unoptimized
    // plans will end up with cross joins, making some of these queries really
    // slow.
    run_with_all_thread_configurations::<TpchBenchSetup>(
        &args,
        "../slt/tpchbench",
        "slt_tpchbench",
    )?;

    Ok(())
}

trait EngineSetup<E, R>: Sync + Send
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    // TODO: Remove passing rt here.
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>>;
}

#[derive(Debug, Clone, Copy)]
struct StandardSetup;

impl<E, R> EngineSetup<E, R> for StandardSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(30),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct TpchGenSetup;

impl<E, R> EngineSetup<E, R> for TpchGenSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(TpchGenExtension)?;
        Ok(RunConfig {
            tokio_rt,
            engine,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct CsvSetup;

impl<E, R> EngineSetup<E, R> for CsvSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(CsvExtension)?;
        Ok(RunConfig {
            tokio_rt,
            engine,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(5),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ParquetSetup;

impl<E, R> EngineSetup<E, R> for ParquetSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(ParquetExtension)?;
        Ok(RunConfig {
            tokio_rt,
            engine,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(5),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct IcebergSetup;

impl<E, R> EngineSetup<E, R> for IcebergSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(IcebergExtension)?;
        Ok(RunConfig {
            tokio_rt,
            engine,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(5),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct HttpSetup;

impl<E, R> EngineSetup<E, R> for HttpSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;
        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct S3PublicSetup;

impl<E, R> EngineSetup<E, R> for S3PublicSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;
        engine.register_extension(IcebergExtension)?;
        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct S3PrivateSetup;

impl<E, R> EngineSetup<E, R> for S3PrivateSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;
        engine.register_extension(IcebergExtension)?;

        let mut vars = ReplacementVars::default();

        vars.add_var("AWS_KEY", VarValue::sensitive_from_env("AWS_KEY"));
        vars.add_var("AWS_SECRET", VarValue::sensitive_from_env("AWS_SECRET"));

        Ok(RunConfig {
            engine,
            tokio_rt,
            vars,
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct GcsPublicSetup;

impl<E, R> EngineSetup<E, R> for GcsPublicSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;
        engine.register_extension(IcebergExtension)?;
        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct GcsPrivateSetup;

impl<E, R> EngineSetup<E, R> for GcsPrivateSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(CsvExtension)?;
        engine.register_extension(ParquetExtension)?;
        engine.register_extension(IcebergExtension)?;

        let mut vars = ReplacementVars::default();

        vars.add_var(
            "GCP_SERVICE_ACCOUNT",
            VarValue::sensitive_from_env("GCP_SERVICE_ACCOUNT"),
        );

        Ok(RunConfig {
            engine,
            tokio_rt,
            vars,
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ClickbenchSingleSetup;

impl<E, R> EngineSetup<E, R> for ClickbenchSingleSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(ParquetExtension)?;

        run_setup_query(
            &engine,
            "
            CREATE TEMP VIEW hits AS
              SELECT * REPLACE (EventDate::DATE AS EventDate)
                FROM read_parquet('../submodules/testdata/clickbench/single/hits_truncated.parquet')
            ",
        )?;

        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(15),
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct ClickbenchPartitionedSetup;

impl<E, R> EngineSetup<E, R> for ClickbenchPartitionedSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(ParquetExtension)?;

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
                FROM read_parquet('../submodules/testdata/clickbench/partitioned/hits_truncated_*.parquet')
            ",
        )?;

        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(30), // Single-partition, unoptimized, debug, not a good time
        })
    }
}

#[derive(Debug, Clone, Copy)]
struct TpchBenchSetup;

impl<E, R> EngineSetup<E, R> for TpchBenchSetup
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    fn setup(tokio_rt: TokioRuntime, engine: SingleUserEngine<E, R>) -> Result<RunConfig<E, R>> {
        engine.register_extension(ParquetExtension)?;

        let tables = [
            "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
        ];

        for table in tables {
            let query = format!(
                "CREATE TEMP VIEW {table} AS SELECT * FROM read_parquet('../submodules/testdata/tpch_sf0.1/{table}.parquet')"
            );
            run_setup_query(&engine, &query)?;
        }

        Ok(RunConfig {
            engine,
            tokio_rt,
            vars: ReplacementVars::default(),
            create_slt_tmp: false,
            query_timeout: Duration::from_secs(30),
        })
    }
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

fn run_with_executor<S>(
    args: &Arguments<SltArguments>,
    executor_fn: impl Fn() -> Result<ThreadedNativeExecutor>,
    path: &str,
    tag: &str,
) -> Result<()>
where
    S: EngineSetup<NativeExecutor<ThreadedScheduler>, NativeSystemRuntime>,
{
    let paths = find_files(Path::new(path), ".slt").unwrap();
    glaredb_slt::run(
        args,
        paths,
        || {
            let tokio_rt = new_tokio_runtime_for_io()?;

            let executor = executor_fn()?;
            let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

            let engine = SingleUserEngine::try_new(executor, runtime.clone())?;
            S::setup(tokio_rt, engine)
        },
        tag,
    )
}

fn run_with_all_thread_configurations<S>(
    args: &Arguments<SltArguments>,
    path: &str,
    tag: &str,
) -> Result<()>
where
    S: EngineSetup<NativeExecutor<ThreadedScheduler>, NativeSystemRuntime>,
{
    // Executor with a the default number of threads (auto-detected).
    run_with_executor::<S>(
        args,
        || ThreadedNativeExecutor::try_new(),
        path,
        &format!("{}/default", tag),
    )?;

    // Executor using a single thread.
    run_with_executor::<S>(
        args,
        || ThreadedNativeExecutor::try_new_with_num_threads(1),
        path,
        &format!("{}/single", tag),
    )?;

    // Executor using a hardcode number of threads.
    run_with_executor::<S>(
        args,
        || ThreadedNativeExecutor::try_new_with_num_threads(16),
        path,
        &format!("{}/multi", tag),
    )?;

    Ok(())
}
