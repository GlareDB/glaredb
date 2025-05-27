mod convert;
mod vars;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::Duration;

use clap::Parser;
use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::datatype::DataTypeId;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::arrays::format::pretty::components::PRETTY_COMPONENTS;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::arrays::format::{BinaryFormat, FormatOptions, Formatter};
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_error::{DbError, Result, ResultExt};
use glaredb_rt_native::runtime::{NativeSystemRuntime, ThreadedNativeExecutor};
use harness::Arguments;
use harness::sqlfile::slt_parser::{ColumnType, ExpectedError, SltRecord, StatementExpect};
use harness::trial::Trial;
use tokio::runtime::Runtime as TokioRuntime;
pub use vars::*;

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
    pub tokio_rt: TokioRuntime,

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
                unimplemented!()
                // match handle.block_on(run_test(args.extra, path, session_fn)) {
                //     Ok(_) => Ok(()),
                //     Err(e) => Err(e.into()),
                // }
            })
            .with_kind(kind)
        })
        .collect();

    harness::run(args, tests).exit_if_failed();

    Ok(())
}

/// Run an SLT at path, creating an engine from the provided function.
fn run_test<F, Fut>(args: SltArguments, path: impl AsRef<Path>, conf_fn: F) -> Result<()>
where
    F: Fn() -> Result<RunConfig<ThreadedNativeExecutor, NativeSystemRuntime>>,
{
    let path = path.as_ref();
    let input = std::fs::read_to_string(path)?;

    let path_str = path.to_string_lossy();
    let records = SltRecord::parse_many(&path_str, &input)?;
    let conf = conf_fn()?;

    for record in records {
        match record {
            SltRecord::Halt(_) => return Ok(()),
            SltRecord::Statement(record) => {
                let sql = record.sql.lines.join("\n");
                let result =
                    conf.tokio_rt
                        .block_on(run_query(args, &conf.engine, sql, conf.query_timeout));

                match (result, record.expected) {
                    (Ok(_), StatementExpect::Ok) => (), // Ok!
                    (Ok(_), StatementExpect::Error(_)) => {
                        return record
                            .loc
                            .emit_error("Expected query to error, but it passed");
                    }
                    (Err(e), StatementExpect::Ok) => {
                        return record.loc.emit_error(format!("Query error: {e}"));
                    }
                    (Err(e), StatementExpect::Error(expect)) => {
                        let err_str = e.to_string();
                        match expect {
                            ExpectedError::Empty => {
                                // We expected an error, we got an error.
                            }
                            ExpectedError::Inline(inline) => {
                                if !err_str.contains(inline) {
                                    let err = record
                                        .loc
                                        .format_error("Error does not contain expected string")
                                        .with_field("error", err_str)
                                        .with_field("expected", inline.to_string());
                                    return Err(err);
                                }
                            }
                            ExpectedError::Multiline(lines) => {
                                let expect_str = lines.join("\n");
                                if !err_str.contains(&expect_str) {
                                    let err = record
                                        .loc
                                        .format_error("Error does not contain expected string")
                                        .with_field("error", err_str)
                                        .with_field("expected", expect_str);
                                    return Err(err);
                                }
                            }
                        }
                    }
                }
            }
            SltRecord::Query(record) => {
                let sql = record.sql.join("\n");
                let result =
                    conf.tokio_rt
                        .block_on(run_query(args, &conf.engine, sql, conf.query_timeout));

                let (schema, batches) = match result {
                    Ok(out) => out,
                    Err(e) => {
                        return Err(record
                            .loc
                            .format_error("Query error")
                            .with_field("error", e));
                    }
                };

                // Check schema matches what we expect.
                let got_types = schema_to_types(&schema);
                if got_types != record.types {
                    return Err(record
                        .loc
                        .format_error("Results types do not match expected types")
                        .with_field(
                            "got",
                            got_types.iter().map(|t| t.to_char()).collect::<String>(),
                        )
                        .with_field(
                            "expected",
                            record.types.iter().map(|t| t.to_char()).collect::<String>(),
                        ));
                }

                //
                unimplemented!()
            }
        }
    }

    Ok(())
}

async fn run_query(
    args: SltArguments,
    engine: &SingleUserEngine<ThreadedNativeExecutor, NativeSystemRuntime>,
    sql: String,
    timeout: Duration,
) -> Result<(ColumnSchema, Vec<Batch>)> {
    // EXPLAIN...
    if args.print_explain {
        let (cols, _rows) = crossterm::terminal::size().unwrap_or((100, 0));

        println!("---- EXPLAIN ----");
        println!("{sql}");

        match engine
            .session()
            .query(&format!("EXPLAIN VERBOSE {sql}"))
            .await
        {
            Ok(mut q_res) => {
                let batches = q_res.output.collect().await.unwrap();
                println!(
                    "{}",
                    PrettyTable::try_new(
                        &q_res.output_schema,
                        &batches,
                        cols as usize,
                        Some(200),
                        PRETTY_COMPONENTS
                    )
                    .unwrap()
                );
            }
            Err(_) => {
                println!("Explain not available");
            }
        };
    }

    // Run the query with a timeout.
    let mut q_res = engine.session().query(&sql).await?;
    let mut timeout = Box::pin(tokio::time::sleep(timeout));

    // Continually read from the stream, erroring if we exceed timeout.
    tokio::select! {
        materialized = q_res.output.collect() => {
            // TODO: Debug print profile
            let batches = materialized?;
            Ok((q_res.output_schema, batches))
        }
        _ = &mut timeout => {
             // Timed out.
            q_res.output.query_handle().cancel();

            Err(DbError::new("Query timed out"))
        }
    }
}

fn schema_to_types(schema: &ColumnSchema) -> Vec<ColumnType> {
    let mut typs = Vec::new();
    for field in &schema.fields {
        let typ = match field.datatype.id() {
            DataTypeId::Int8
            | DataTypeId::Int16
            | DataTypeId::Int32
            | DataTypeId::Int64
            | DataTypeId::UInt8
            | DataTypeId::UInt16
            | DataTypeId::UInt32
            | DataTypeId::UInt64 => ColumnType::Integer,
            DataTypeId::Float32 | DataTypeId::Float64 => ColumnType::Float,
            DataTypeId::Utf8 | DataTypeId::Boolean => ColumnType::Text,
            _ => ColumnType::Any,
        };
        typs.push(typ);
    }

    typs
}

/// Converts batches into rows of columns.
fn batches_to_rows(batches: Vec<Batch>) -> Result<Vec<Vec<String>>> {
    const OPTS: FormatOptions = FormatOptions {
        null: "NULL",
        empty_string: "(empty)",
        binary_format: BinaryFormat::Hex,
    };
    let formatter = Formatter::new(OPTS);

    let mut rows = Vec::new();

    for batch in batches {
        for row_idx in 0..batch.num_rows() {
            let col_strings = batch
                .arrays()
                .iter()
                .map(|arr| {
                    formatter
                        .format_array_value(arr, row_idx)
                        .map(|v| v.to_string())
                })
                .collect::<Result<Vec<_>>>()?;

            rows.push(col_strings)
        }
    }

    Ok(rows)
}
