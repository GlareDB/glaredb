mod vars;
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use harness::Arguments;
use harness::trial::Trial;
use uuid::Uuid;
pub use vars::*;

mod convert;

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use clap::Parser;
use convert::{batches_to_rows, schema_to_types};
use glaredb_error::{DbError, Result, ResultExt};
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};
use sqllogictest::DefaultColumnType;
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Debug, Parser, Clone, Copy)]
pub struct SltArguments {
    /// Print the EXPLAIN output of a test query before running it.
    #[clap(long, env = "PRINT_EXPLAIN")]
    pub print_explain: bool,
    /// Print out the profile data for test queries.
    #[clap(long, env = "PRINT_PROFILE_DATA")]
    pub print_profile_data: bool,
}

#[derive(Debug)]
pub struct RunConfig<E, R>
where
    E: PipelineRuntime,
    R: SystemRuntime,
{
    /// The session to use for this run.
    pub engine: SingleUserEngine<E, R>,

    /// Variables to replace in the query.
    ///
    /// Variables are shared across all runs for a single "test" (multiple
    /// files).
    pub vars: ReplacementVars,

    /// Create the slt tmp dir that the variable '__SLT_TMP__' points to.
    ///
    /// If false, the directory won't be created, but the '__SLT_TMP__' will
    /// still be populated, which allows for testing if a certain action can
    /// create a directory.
    pub create_slt_tmp: bool,

    /// Max duration a query can be executing before being canceled.
    pub query_timeout: Duration,
}

/// Run all SLTs from the provided paths.
///
/// This sets up tracing to log only at the ERROR level. RUST_LOG can be used to
/// print out logs at a lower level.
///
/// For each path, `session_fn` will be called to create a session (and
/// associated configuration) for just the file.
///
/// `kind` should be used to group these SLTs together.
pub fn run<F, Fut>(
    args: &Arguments<SltArguments>,
    paths: impl IntoIterator<Item = PathBuf>,
    engine_fn: F,
    kind: &str,
) -> Result<()>
where
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>>>,
{
    let tokio = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_io()
        .enable_time()
        .thread_name("rayexec_slt")
        .build()
        .context("Failed to build tokio runtime")?;

    let handle = tokio.handle();

    let tests = paths
        .into_iter()
        .map(|path| {
            let test_name = path.to_string_lossy().to_string();
            let test_name = test_name.trim_start_matches("../");
            let session_fn = engine_fn.clone();
            let handle = handle.clone();
            Trial::test(test_name, move || {
                match handle.block_on(run_test(args.extra, path, session_fn)) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
                }
            })
            .with_kind(kind)
        })
        .collect();

    harness::run(args, tests).exit_if_failed();

    Ok(())
}

/// Run an SLT at path, creating an engine from the provided function.
async fn run_test<F, Fut>(args: SltArguments, path: impl AsRef<Path>, session_fn: F) -> Result<()>
where
    F: Fn() -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>>>,
{
    let path = path.as_ref();

    let mut runner = sqllogictest::Runner::new(|| async {
        let conf = session_fn().await?;

        Ok(TestSession { args, conf })
    });
    runner
        .run_file_async(path)
        .await
        .context("Failed to run SLT")?;
    Ok(())
}

#[derive(Debug)]
#[allow(dead_code)]
struct TestSession {
    args: SltArguments,
    conf: RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>,
}

impl TestSession {
    async fn debug_explain(&mut self, sql: &str) {
        if !self.args.print_explain {
            // Not set.
            return;
        }

        let (cols, _rows) = crossterm::terminal::size().unwrap_or((100, 0));

        println!("---- EXPLAIN ----");
        println!("{sql}");

        let mut query_res = match self
            .conf
            .engine
            .session()
            .query(&format!("EXPLAIN VERBOSE {sql}"))
            .await
        {
            Ok(results) => results,
            Err(_) => {
                println!("Explain not available");
                return;
            }
        };

        let batches = query_res.output.collect().await.unwrap();
        println!(
            "{}",
            PrettyTable::try_new(
                &query_res.output_schema,
                &batches,
                cols as usize,
                Some(200),
                PRETTY_COMPONENTS
            )
            .unwrap()
        );
    }

    async fn debug_print_profile(&self, query_id: Uuid) {
        if !self.args.print_profile_data {
            // Not set.
            return;
        }

        let (cols, _rows) = crossterm::terminal::size().unwrap_or((100, 0));
        let run_and_print = async |query: String| {
            if let Ok(mut res) = self.conf.engine.session().query(&query).await {
                let batches = res.output.collect().await.unwrap();
                println!(
                    "{}",
                    PrettyTable::try_new(
                        &res.output_schema,
                        &batches,
                        cols as usize,
                        Some(200),
                        PRETTY_COMPONENTS
                    )
                    .unwrap()
                );
            }
        };

        println!("---- PLANNING PROFILE ----");
        run_and_print(format!("SELECT * FROM planning_profile('{query_id}')")).await;

        println!("---- EXECUTION PROFILE ----");
        run_and_print(format!("SELECT * FROM execution_profile('{query_id}')")).await;
    }

    async fn run_inner(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<DefaultColumnType>, DbError> {
        info!(%sql, "query");

        let mut sql_with_replacements = sql.to_string();
        for (k, v) in self.conf.vars.iter() {
            if k == "__SLT_TMP__" && self.conf.create_slt_tmp {
                std::fs::create_dir_all(v.as_ref()).context("failed to create slt tmp dir")?
            }

            sql_with_replacements = sql_with_replacements.replace(k, v.as_ref());
        }

        self.debug_explain(&sql_with_replacements).await;

        let mut query_res = self
            .conf
            .engine
            .session()
            .query(&sql_with_replacements)
            .await?;

        // Timeout for the entire query.
        let mut timeout = Box::pin(tokio::time::sleep(self.conf.query_timeout));

        // Continually read from the stream, erroring if we exceed timeout.
        tokio::select! {
            materialized = query_res.output.collect() => {
                let materialized = materialized?;
                self.debug_print_profile(query_res.query_id).await;

                Ok(sqllogictest::DBOutput::Rows {
                    types: schema_to_types(&query_res.output_schema),
                    rows: batches_to_rows(materialized)?,
                })
            }
            _ = &mut timeout => {
                 // Timed out.
                query_res.output.query_handle().cancel();

                // let prof_data = handle.generate_execution_profile_data().await.unwrap();
                Err(DbError::new(format!(
                    "Variables\n{}\nQuery timed out\n---",
                    self.conf.vars
                )))
            }
        }
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for TestSession {
    type Error = DbError;
    type ColumnType = DefaultColumnType;

    async fn run(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<Self::ColumnType>, Self::Error> {
        self.run_inner(sql).await
    }

    fn engine_name(&self) -> &str {
        "rayexec"
    }
}
