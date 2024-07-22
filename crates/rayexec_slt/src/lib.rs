use async_trait::async_trait;
use futures::StreamExt;
use libtest_mimic::{Arguments, Trial};
use rand::{distributions::Alphanumeric, Rng};
use rayexec_bullet::{
    batch::Batch,
    datatype::DataType,
    field::Schema,
    format::{FormatOptions, Formatter},
};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::{
    engine::{session::Session, Engine},
    runtime::ExecutionRuntime,
};
use rayexec_rt_native::runtime::ThreadedExecutionRuntime;
use sqllogictest::DefaultColumnType;
use std::{collections::HashMap, fs, sync::Arc};
use std::{
    fmt,
    path::{Path, PathBuf},
    time::Duration,
};
use tracing_subscriber::{EnvFilter, FmtSubscriber};

#[derive(Clone)]
pub enum VarValue {
    /// Value is sensitive, don't print it out.
    Sensitive(String),
    /// Value is not sensitive, print it during debugging.
    Plain(String),
}

impl VarValue {
    pub fn plain_from_env(key: &str) -> VarValue {
        match std::env::var(key) {
            Ok(s) => VarValue::Plain(s),
            Err(_) => {
                println!("Missing environment variable: {key}");
                std::process::exit(2);
            }
        }
    }

    pub fn sensitive_from_env(key: &str) -> VarValue {
        match std::env::var(key) {
            Ok(s) => VarValue::Sensitive(s),
            Err(_) => {
                println!("Missing environment variable: {key}");
                std::process::exit(2);
            }
        }
    }
}

impl fmt::Display for VarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sensitive(_) => write!(f, "***"),
            VarValue::Plain(s) => write!(f, "{s}"),
        }
    }
}

impl fmt::Debug for VarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl AsRef<str> for VarValue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Sensitive(s) => s.as_str(),
            VarValue::Plain(s) => s.as_str(),
        }
    }
}

/// Variables that can be referenced in sql queries and automatically replaced
/// with concrete values.
///
/// Variable format in sql queries: __MYVARIABLE__
///
/// When adding a variable, they'll automatically be uppercased and surrounded
/// with understores.
///
/// See default implementation for predefined variables.
///
/// A new instance of these variables is created for each file run.
#[derive(Debug, Clone)]
pub struct ReplacementVars {
    vars: HashMap<String, VarValue>,
}

impl Default for ReplacementVars {
    fn default() -> Self {
        let mut vars = ReplacementVars {
            vars: HashMap::new(),
        };

        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        // Dir (relative to test_bin) where tests can write temp files for
        // things like COPY TO.
        vars.add_var("SLT_TMP", VarValue::Plain(format!("../slt_tmp/{s}")));

        vars
    }
}

impl ReplacementVars {
    pub fn add_var(&mut self, key: &str, val: VarValue) {
        let key = format!("__{}__", key.to_uppercase());
        self.vars.insert(key, val);
    }

    fn replace_in_query(&self, query: impl Into<String>, conf: &RunConfig) -> Result<String> {
        let mut query = query.into();

        for (k, v) in &self.vars {
            if k == "__SLT_TMP__" && conf.create_slt_tmp {
                std::fs::create_dir_all(v.as_ref()).context("failed to create slt tmp dir")?
            }

            query = query.replace(k, v.as_ref());
        }

        Ok(query)
    }
}

impl fmt::Display for ReplacementVars {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (k, v) in &self.vars {
            writeln!(f, "{k} = {v}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunConfig {
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
/// For each path, `engine_fn` will be called to create an engine (and
/// associated session) for just the file. An engine runtime will be provided.
///
/// `kind` should be used to group these SLTs together.
pub fn run<F>(
    paths: impl IntoIterator<Item = PathBuf>,
    engine_fn: F,
    conf: RunConfig,
    kind: &str,
) -> Result<()>
where
    F: Fn(Arc<dyn ExecutionRuntime>) -> Result<Engine> + Clone + Send + 'static,
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

    let rt = Arc::new(ThreadedExecutionRuntime::try_new()?.with_default_tokio()?);

    let tests = paths
        .into_iter()
        .map(|path| {
            let test_name = path.to_string_lossy().to_string();
            let test_name = test_name.trim_start_matches("../");
            let rt = rt.clone();
            let engine_fn = engine_fn.clone();
            let conf = conf.clone();
            Trial::test(test_name, move || {
                match rt
                    .clone()
                    .tokio_handle()
                    .expect("tokio to be configured")
                    .block_on(run_test(path, rt, engine_fn, conf))
                {
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
async fn run_test(
    path: impl AsRef<Path>,
    rt: Arc<dyn ExecutionRuntime>,
    engine_fn: impl Fn(Arc<dyn ExecutionRuntime>) -> Result<Engine>,
    conf: RunConfig,
) -> Result<()> {
    let path = path.as_ref();

    let mut runner = sqllogictest::Runner::new(|| async {
        let engine = engine_fn(rt.clone())?;
        let session = engine.new_session()?;

        Ok(TestSession {
            conf: conf.clone(),
            session,
            engine,
        })
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
    engine: Engine,
    session: Session,
}

impl TestSession {
    async fn run_inner(
        &mut self,
        sql: &str,
    ) -> Result<sqllogictest::DBOutput<DefaultColumnType>, RayexecError> {
        let sql = self.conf.vars.replace_in_query(sql, &self.conf)?;

        let mut rows = Vec::new();
        let mut results = self.session.simple(&sql).await?;
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

/// Convert a batch into a vector of rows.
fn batch_to_rows(batch: Batch) -> Result<Vec<Vec<String>>> {
    const OPTS: FormatOptions = FormatOptions {
        null: "NULL",
        empty_string: "(empty)",
    };
    let formatter = Formatter::new(OPTS);

    let mut rows = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let row = batch.row(row_idx).expect("row to exist");

        let col_strings: Vec<_> = row
            .iter()
            .map(|col| formatter.format_scalar_value(col.clone()).to_string())
            .collect();

        match transform_multiline_cols_to_rows(&col_strings) {
            Some(new_rows) => rows.extend(new_rows),
            None => rows.push(col_strings),
        }
    }

    Ok(rows)
}

/// Transforms a row with a potentially multiline column into multiple rows with
/// each column containing a single line.
///
/// Columns that don't have content for that row will instead have a '.'.
///
/// For example, the following output:
/// ```text
/// +-------------+---------------------------------------------+
/// | type        | plan                                        |
/// +-------------+---------------------------------------------+
/// | logical     | Order (expressions = [#0 DESC NULLS FIRST]) |
/// |             |   Projection (expressions = [#0])           |
/// |             |     ExpressionList                          |
/// +-------------+---------------------------------------------+
/// | pipeline    | Pipeline 1                                  |
/// |             |   Pipeline 2                                |
/// +-------------+---------------------------------------------+
/// ```
///
/// Would get trasnformed into:
/// ```text
/// logical   Order (expressions = [#0 DESC NULLS FIRST])
/// .           Projection (expressions = [#0])
/// .             ExpressionList
/// pipeline  Pipeline 1
/// .           Pipeline 2
/// ```
/// Where each line is a new "row".
///
/// This allows for nicely formatted SLTs for queries that return multiline
/// results (like EXPLAIN).
fn transform_multiline_cols_to_rows<S: AsRef<str>>(cols: &[S]) -> Option<Vec<Vec<String>>> {
    let max = cols.iter().fold(0, |curr, col| {
        let col_lines = col.as_ref().lines().count();
        if col_lines > curr {
            col_lines
        } else {
            curr
        }
    });

    if max > 1 {
        let mut new_rows = Vec::new();
        for row_idx in 0..max {
            let new_row: Vec<_> = cols
                .iter()
                .map(|col| col.as_ref().lines().nth(row_idx).unwrap_or(".").to_string())
                .collect();
            new_rows.push(new_row)
        }
        Some(new_rows)
    } else {
        None
    }
}

fn schema_to_types(schema: &Schema) -> Vec<DefaultColumnType> {
    let mut typs = Vec::new();
    for field in &schema.fields {
        let typ = match field.datatype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => DefaultColumnType::Integer,
            DataType::Float32 | DataType::Float64 => DefaultColumnType::FloatingPoint,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean => DefaultColumnType::Text,
            _ => DefaultColumnType::Any,
        };
        typs.push(typ);
    }

    typs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transform_not_needed() {
        let orig_row = &["col1", "col2", "col3"];
        let out = transform_multiline_cols_to_rows(orig_row);
        assert_eq!(None, out);
    }

    #[test]
    fn transform_multiline_col() {
        let orig_row = &["col1", "col2\ncol2a\ncol2b", "col3"];
        let out = transform_multiline_cols_to_rows(orig_row);

        let expected = vec![
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            vec![".".to_string(), "col2a".to_string(), ".".to_string()],
            vec![".".to_string(), "col2b".to_string(), ".".to_string()],
        ];

        assert_eq!(Some(expected), out);
    }

    #[test]
    fn transform_multiple_multiline_cols() {
        let orig_row = &["col1", "col2\ncol2a\ncol2b", "col3\ncol3a"];
        let out = transform_multiline_cols_to_rows(orig_row);

        let expected = vec![
            vec!["col1".to_string(), "col2".to_string(), "col3".to_string()],
            vec![".".to_string(), "col2a".to_string(), "col3a".to_string()],
            vec![".".to_string(), "col2b".to_string(), ".".to_string()],
        ];

        assert_eq!(Some(expected), out);
    }
}
