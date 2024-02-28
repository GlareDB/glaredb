use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use arrow_util::pretty;
use clap::ValueEnum;
use colored::Colorize;
use datafusion::arrow::csv::writer::WriterBuilder as CsvWriterBuilder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{
    JsonFormat,
    LineDelimited as JsonLineDelimted,
    Writer as JsonWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion_ext::vars::SessionVars;
use futures::StreamExt;
use pgrepr::format::Format;
use pgrepr::notice::NoticeSeverity;
use reedline::{FileBackedHistory, Reedline, Signal};
use sqlbuiltins::functions::scalars::glaredb_ffl::GlaredbFFIPlugin;
use sqlbuiltins::functions::BuiltinScalarUDF;
use sqlexec::engine::{Engine, SessionStorageConfig, TrackedSession};
use sqlexec::remote::client::{RemoteClient, RemoteClientType};
use sqlexec::session::ExecutionResult;
use url::Url;

use crate::args::{LocalClientOpts, OutputMode, StorageConfigArgs};
use crate::highlighter::{SQLHighlighter, SQLHinter, SQLValidator};
use crate::prompt::SQLPrompt;

#[derive(Debug, Clone, Copy)]
enum ClientCommandResult {
    /// Exit the program.
    Exit,
    /// Continue on.
    Continue,
}

pub struct LocalSession {
    sess: TrackedSession,
    _engine: Engine,
    opts: LocalClientOpts,
}

impl LocalSession {
    pub async fn connect(opts: LocalClientOpts) -> Result<Self> {
        // Connect to metastore.
        let mut engine = if let StorageConfigArgs {
            location: Some(location),
            storage_options,
        } = &opts.storage_config
        {
            // TODO: try to consolidate with --data-dir option
            Engine::from_storage_options(location, &HashMap::from_iter(storage_options.clone()))
                .await?
        } else {
            Engine::from_data_dir(opts.data_dir.as_ref()).await?
        };

        engine = engine.with_spill_path(opts.spill_path.clone());

        let sess = if let Some(url) = opts.cloud_url.clone() {
            let (exec_client, info_msg) = if opts.ignore_rpc_auth {
                let u = &url.to_string();
                (
                    RemoteClient::connect(url).await?,
                    format!("Connected to remote GlareDB server: {}", u.cyan()),
                )
            } else {
                let client = RemoteClient::connect_with_proxy_destination(
                    url.try_into()?,
                    opts.cloud_addr.clone(),
                    opts.disable_tls,
                    RemoteClientType::Cli,
                )
                .await?;

                let msg = format!(
                    "Connected to Cloud deployment (TLS {}): {}",
                    if opts.disable_tls {
                        "disabled"
                    } else {
                        "enabled"
                    },
                    client.get_deployment_name().cyan()
                );

                (client, msg)
            };
            let mut sess = engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await?;

            sess.attach_remote_session(exec_client.clone(), None)
                .await?;

            println!("{}", info_msg);

            sess
        } else {
            let mut sess = engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await?;
            let f = GlaredbFFIPlugin::try_new(
                "ffi_echo",
                "/Users/corygrinstead/Development/glaredb_extension/target/debug/libexpression_lib.dylib",
                "echo",
                None
            ).unwrap();

            let f: Arc<dyn BuiltinScalarUDF> = Arc::new(f);
            sess.register_function(f).await.unwrap();
            sess
        };

        Ok(LocalSession {
            sess,
            _engine: engine,
            opts,
        })
    }

    pub async fn run(mut self, query: Option<String>) -> Result<()> {
        if let Some(query) = query {
            self.execute_one(&query).await
        } else {
            self.run_interactive().await
        }
    }

    async fn run_interactive(&mut self) -> Result<()> {
        match (&self.opts.storage_config, &self.opts.data_dir) {
            (
                StorageConfigArgs {
                    location: Some(location),
                    ..
                },
                _,
            ) => println!("Persisting database at location: {location}"),
            (_, Some(path)) => println!("Persisting database at path: {}", path.display()),
            _ => (),
        };

        println!("Type {} for help.", "\\help".bold().italic());

        let history = Box::new(
            FileBackedHistory::with_file(10000, get_history_path())
                .expect("Error configuring history with file"),
        );

        let mut line_editor = Reedline::create()
            .with_history(history)
            .with_hinter(Box::new(SQLHinter::new()))
            .with_highlighter(Box::new(SQLHighlighter))
            .with_validator(Box::new(SQLValidator));

        let prompt = SQLPrompt {};

        loop {
            let sig = line_editor.read_line(&prompt);
            match sig {
                Ok(Signal::Success(buffer)) => match buffer.as_str() {
                    cmd if is_client_cmd(cmd) => match self.handle_client_cmd(cmd).await {
                        Ok(ClientCommandResult::Continue) => (),
                        Ok(ClientCommandResult::Exit) => return Ok(()),
                        Err(e) => {
                            println!("Error: {e}")
                        }
                    },
                    _ => {
                        match self.execute(&buffer).await {
                            Ok(_) => {}
                            Err(e) => println!("Error: {e}"),
                        };

                        // Print out notices as needed.
                        //
                        // Note this isn't being called in the above `execute`
                        // function since that can be called in a
                        // non-interactive fashion which and having notice
                        // messages interspersed with the output would be
                        // annoying.
                        for notice in self.sess.take_notices() {
                            eprintln!(
                                "{}: {}",
                                match notice.severity {
                                    s @ (NoticeSeverity::Warning | NoticeSeverity::Error) =>
                                        s.to_string().red(),
                                    other => other.to_string().blue(),
                                },
                                notice.message
                            );
                        }
                    }
                },
                Ok(Signal::CtrlD) => break,
                Ok(Signal::CtrlC) => {}
                Err(e) => {
                    return Err(anyhow!("Unable to read from prompt: {e}"));
                }
            }
        }
        Ok(())
    }

    async fn execute_one(&mut self, query: &str) -> Result<()> {
        self.execute(query).await?;
        Ok(())
    }

    pub async fn execute(&mut self, text: &str) -> Result<()> {
        if is_client_cmd(text) {
            self.handle_client_cmd(text).await?;
            return Ok(());
        }

        let now = if self.opts.timing {
            Some(Instant::now())
        } else {
            None
        };

        const UNNAMED: String = String::new();

        let statements = self.sess.parse_query(text)?;
        for stmt in statements {
            self.sess
                .prepare_statement(UNNAMED, stmt, Vec::new())
                .await?;
            let prepared = self.sess.get_prepared_statement(&UNNAMED)?;
            let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
            self.sess.bind_statement(
                UNNAMED,
                &UNNAMED,
                Vec::new(),
                vec![Format::Text; num_fields],
            )?;

            let stream = self.sess.execute_portal(&UNNAMED, 0).await?;

            match stream {
                ExecutionResult::Query { stream, .. } => {
                    print_stream(
                        stream,
                        self.opts.mode,
                        self.opts.max_width,
                        self.opts.max_rows,
                        now,
                    )
                    .await?
                }
                res @ (ExecutionResult::CopySuccess
                | ExecutionResult::DeleteSuccess { .. }
                | ExecutionResult::InsertSuccess { .. }
                | ExecutionResult::UpdateSuccess { .. }) => {
                    println!("{}", res);
                    print_time_elapsed(now);
                }

                other => {
                    println!("{}", other);
                }
            }
        }
        Ok(())
    }

    async fn handle_client_cmd(&mut self, text: &str) -> Result<ClientCommandResult> {
        let mut ss = text.split_whitespace();
        let cmd = ss.next().unwrap();
        let val = ss.next();

        match (cmd, val) {
            ("\\help", None) => {
                print!("{}", LocalClientOpts::help_string()?);
            }
            ("\\mode", Some(val)) => {
                self.opts.mode = OutputMode::from_str(val, true)
                    .map_err(|s| anyhow!("Unable to set output mode: {s}"))?;
            }
            ("\\max-rows", Some(val)) => self.opts.max_rows = Some(val.parse()?),
            ("\\max-width", Some(val)) => self.opts.max_width = Some(val.parse()?),
            ("\\open", Some(path)) => {
                if let Ok(url) = Url::parse(path) {
                    let new_opts = LocalClientOpts {
                        data_dir: None,
                        cloud_url: Some(url),
                        ..self.opts.clone()
                    };
                    let new_sess = LocalSession::connect(new_opts).await?;
                    *self = new_sess;
                } else {
                    let new_opts = LocalClientOpts {
                        data_dir: Some(PathBuf::from(path)),
                        cloud_url: None,
                        ..self.opts.clone()
                    };
                    let new_sess = LocalSession::connect(new_opts).await?;
                    *self = new_sess;
                }
            }
            ("\\timing", None) => {
                self.opts.timing = !self.opts.timing;
                println!("Timing is {}", if self.opts.timing { "on" } else { "off" })
            }
            ("\\quit", None) | ("\\q", None) | ("exit", None) => {
                return Ok(ClientCommandResult::Exit)
            }
            (cmd, _) => return Err(anyhow!("Unable to handle client command: {cmd}")),
        }

        Ok(ClientCommandResult::Continue)
    }
}

async fn process_stream(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    let batches = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    Ok(batches)
}

async fn print_stream(
    stream: SendableRecordBatchStream,
    mode: OutputMode,
    max_width: Option<usize>,
    max_rows: Option<usize>,
    maybe_now: Option<Instant>,
) -> Result<()> {
    let schema = stream.schema();
    let batches = process_stream(stream).await?;

    fn write_json<F: JsonFormat>(batches: &[RecordBatch]) -> Result<()> {
        let stdout = std::io::stdout();
        let buf = std::io::BufWriter::new(stdout);
        let mut writer = JsonWriter::<_, F>::new(buf);
        for batch in batches {
            writer.write(batch)?;
        }
        writer.finish()?;
        let mut buf = writer.into_inner();
        buf.flush()?;
        Ok(())
    }

    match mode {
        OutputMode::Table => {
            // If width not explicitly set by the user, try to get the width of ther
            // terminal.
            let width = max_width.unwrap_or(terminal_util::term_width());
            let disp = pretty::pretty_format_batches(&schema, &batches, Some(width), max_rows)?;
            println!("{disp}");
        }
        OutputMode::Csv => {
            let stdout = std::io::stdout();
            let buf = std::io::BufWriter::new(stdout);
            let mut writer = CsvWriterBuilder::new().with_header(true).build(buf);
            for batch in batches {
                writer.write(&batch)?; // CSV writer flushes per write.
            }
        }
        OutputMode::Json => write_json::<JsonArrayNewLines>(&batches)?,
        OutputMode::Ndjson => write_json::<JsonLineDelimted>(&batches)?,
    }
    print_time_elapsed(maybe_now);

    Ok(())
}

pub(crate) fn print_time_elapsed(maybe_now: Option<Instant>) {
    if let Some(now) = maybe_now {
        println!("Time: {:.3}s", now.elapsed().as_secs_f64());
    }
}

pub(crate) fn is_client_cmd(s: &str) -> bool {
    s.starts_with('\\') || s == "exit"
}

/// Produces JSON output as a single JSON array with new lines between objects.
///
/// ```json
/// [{"foo":1},
/// {"bar":1}]
/// ```
#[derive(Debug, Default)]
pub struct JsonArrayNewLines {}

impl JsonFormat for JsonArrayNewLines {
    fn start_stream<W: Write>(&self, writer: &mut W) -> Result<(), ArrowError> {
        writer.write_all(b"[")?;
        Ok(())
    }

    fn start_row<W: Write>(&self, writer: &mut W, is_first_row: bool) -> Result<(), ArrowError> {
        if !is_first_row {
            writer.write_all(b",\n")?;
        }
        Ok(())
    }

    fn end_stream<W: Write>(&self, writer: &mut W) -> Result<(), ArrowError> {
        writer.write_all(b"]\n")?;
        Ok(())
    }
}

fn get_home_dir() -> PathBuf {
    match env::var("HOME") {
        Ok(path) => PathBuf::from(path),
        Err(_) => match env::var("USERPROFILE") {
            Ok(path) => PathBuf::from(path),
            Err(_) => panic!("Failed to get home directory"),
        },
    }
}

fn get_history_path() -> PathBuf {
    let mut home_dir = get_home_dir();
    home_dir.push(".glaredb");
    home_dir.push("history.txt");
    home_dir
}
