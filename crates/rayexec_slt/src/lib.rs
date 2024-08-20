mod vars;
pub use vars::*;

mod convert;

use async_trait::async_trait;
use convert::{batch_to_rows, schema_to_types};
use futures::StreamExt;
use libtest_mimic::{Arguments, Trial};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::engine::session::Session;
use rayexec_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};
use sqllogictest::DefaultColumnType;
use std::fs;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tracing::info;
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Debug)]
pub struct RunConfig {
    /// The session to use for this run.
    pub session: Session<ThreadedNativeExecutor, NativeRuntime>,

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
pub fn run<F>(paths: impl IntoIterator<Item = PathBuf>, session_fn: F, kind: &str) -> Result<()>
where
    F: Fn() -> Result<RunConfig> + Clone + Send + 'static,
{
    let args = Arguments::from_args();
    let env_filter = EnvFilter::builder()
        .with_default_directive(tracing::Level::ERROR.into())
        .from_env_lossy()
        .add_directive("h2=info".parse().unwrap())
        .add_directive("hyper=info".parse().unwrap())
        .add_directive("sqllogictest=info".parse().unwrap());
    let subscriber = FmtSubscriber::builder()
        .with_test_writer() // TODO: Actually capture
        .with_env_filter(env_filter)
        .with_file(true)
        .with_line_number(true)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    std::panic::set_hook(Box::new(|info| {
        let backtrace = std::backtrace::Backtrace::force_capture();
        println!("---- PANIC ----\nInfo: {}\n\nBacktrace:{}", info, backtrace);
        std::process::abort();
    }));

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
            let session_fn = session_fn.clone();
            let handle = handle.clone();
            Trial::test(test_name, move || {
                match handle.block_on(run_test(path, session_fn)) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(e.into()),
                }
            })
            .with_kind(kind)
        })
        .collect();

    libtest_mimic::run(&args, tests).exit_if_failed();

    Ok(())
}

/// Recursively find all files in the given directory.
pub fn find_files(dir: &Path) -> Result<Vec<PathBuf>> {
    fn inner(dir: &Path, paths: &mut Vec<PathBuf>) -> Result<()> {
        if dir.is_dir() {
            for entry in fs::read_dir(dir).context("read dir")? {
                let entry = entry.context("entry")?;
                let path = entry.path();
                if path.is_dir() {
                    inner(&path, paths)?;
                } else {
                    paths.push(path.to_path_buf());
                }
            }
        }
        Ok(())
    }

    let mut paths = Vec::new();
    inner(dir, &mut paths)?;

    Ok(paths)
}

/// Run an SLT at path, creating an engine from the provided function.
async fn run_test<F>(path: impl AsRef<Path>, session_fn: F) -> Result<()>
where
    F: Fn() -> Result<RunConfig> + Clone + Send + 'static,
{
    let path = path.as_ref();

    let mut runner = sqllogictest::Runner::new(|| async {
        let conf = session_fn()?;

        Ok(TestSession { conf })
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
    conf: RunConfig,
}

impl TestSession {
    async fn run_inner(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<DefaultColumnType>, RayexecError> {
        info!(%sql, "query");

        let mut sql = sql.to_string();
        for (k, v) in self.conf.vars.iter() {
            if k == "__SLT_TMP__" && self.conf.create_slt_tmp {
                std::fs::create_dir_all(v.as_ref()).context("failed to create slt tmp dir")?
            }

            sql = sql.replace(k, v.as_ref());
        }

        let mut rows = Vec::new();
        let mut results = self.conf.session.simple(&sql).await?;
        if results.len() != 1 {
            return Err(RayexecError::new(format!(
                "Unexpected number of results for '{sql}': {}",
                results.len()
            )));
        }

        let typs = schema_to_types(&results[0].output_schema);

        loop {
            // Each pull on the stream has a 5 sec timeout. If it takes longer than
            // 5 secs, we can assume that the query is stuck.
            let timeout = tokio::time::timeout(Duration::from_secs(5), results[0].stream.next());

            match timeout.await {
                Ok(Some(result)) => {
                    let batch = result?;
                    rows.extend(batch_to_rows(batch)?);
                }
                Ok(None) => break,
                Err(_) => {
                    // Timed out.
                    results[0].handle.cancel();

                    let dump = results[0].handle.dump();
                    return Err(RayexecError::new(format!(
                        "Variables\n{}\nQuery timed out\n---\n{dump}",
                        self.conf.vars
                    )));
                }
            }
        }

        Ok(sqllogictest::DBOutput::Rows { types: typs, rows })
    }
}

#[async_trait]
impl sqllogictest::AsyncDB for TestSession {
    type Error = RayexecError;
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