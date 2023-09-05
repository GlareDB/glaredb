use crate::args::{LocalClientOpts, OutputMode};
use crate::highlighter::{SQLHighlighter, SQLHinter, SQLValidator};
use crate::prompt::SQLPrompt;
use crate::util::MetastoreClientMode;
use anyhow::{anyhow, Result};
use arrow_util::pretty::pretty_format_batches;
use clap::ValueEnum;
use colored::Colorize;
use datafusion::arrow::csv::writer::WriterBuilder as CsvWriterBuilder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{
    JsonFormat, LineDelimited as JsonLineDelimted, Writer as JsonWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use pgrepr::format::Format;
use reedline::{FileBackedHistory, Reedline, Signal};

use datafusion_ext::vars::SessionVars;
use sqlexec::engine::EngineStorageConfig;
use sqlexec::engine::{Engine, SessionStorageConfig, TrackedSession};
use sqlexec::parser;
use sqlexec::remote::client::RemoteClient;
use sqlexec::session::ExecutionResult;
use std::env;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use telemetry::Tracker;
use tracing::error;
use url::Url;

#[derive(Debug, Clone, Copy)]
enum ClientCommandResult {
    /// Exit the program.
    Exit,
    /// Continue on.
    Continue,
}

pub struct LocalSession {
    sess: TrackedSession,
    engine: Engine,
    opts: LocalClientOpts,
}

impl LocalSession {
    pub async fn connect(opts: LocalClientOpts) -> Result<Self> {
        if opts.metastore_addr.is_some() && opts.data_dir.is_some() {
            return Err(anyhow!(
                "Cannot specify both a metastore address and a local file path"
            ));
        }

        // Connect to metastore.
        let mode = MetastoreClientMode::new_from_options(
            opts.metastore_addr.clone(),
            opts.data_dir.clone(),
        )?;
        let metastore_client = mode.into_client().await?;
        let tracker = Arc::new(Tracker::Nop);

        let storage_conf = match &opts.data_dir {
            Some(path) => EngineStorageConfig::Local { path: path.clone() },
            None => EngineStorageConfig::Memory,
        };

        let engine = Engine::new(
            metastore_client,
            storage_conf,
            tracker,
            opts.spill_path.clone(),
        )
        .await?;

        let sess = if let Some(url) = opts.cloud_url.clone() {
            let (exec_client, info_msg) = if opts.ignore_rpc_auth {
                let u = &url.to_string();
                (
                    RemoteClient::connect(url).await?,
                    format!("Connected to remote GlareDB server: {}", u.bright_cyan()),
                )
            } else {
                let client = RemoteClient::connect_with_proxy_destination(url.try_into()?).await?;

                let msg = format!(
                    "Connected to Cloud deployment: {}",
                    client.get_deployment_name().bright_cyan()
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
            engine
                .new_local_session_context(SessionVars::default(), SessionStorageConfig::default())
                .await?
        };

        Ok(LocalSession { sess, engine, opts })
    }

    pub async fn run(mut self, query: Option<String>) -> Result<()> {
        let result = if let Some(query) = query {
            self.execute_one(&query).await
        } else {
            self.run_interactive().await
        };

        // Wait for the session to close.
        if let Err(err) = self.sess.close().await {
            error!(%err, "unable to close the remote session");
        }

        // Try to shutdown the engine gracefully.
        if let Err(err) = self.engine.shutdown().await {
            error!(%err, "unable to shutdown the engine gracefully");
        }

        result
    }

    async fn run_interactive(&mut self) -> Result<()> {
        let info = match (&self.opts.metastore_addr, &self.opts.data_dir) {
            (Some(addr), None) => {
                format!("Persisting catalog on remote metastore: {}", addr.bold())
            } // TODO: Should we continue to allow this?
            (None, Some(path)) => format!("Persisting database at path: {}", path.display()),
            (None, None) => "Using in-memory catalog".to_string(),
            _ => unreachable!(),
        };

        println!("{info}");

        println!("Type {} for help.", "\\help".bold().italic());

        let history = Box::new(
            FileBackedHistory::with_file(100, get_history_path())
                .expect("Error configuring history with file"),
        );

        let mut line_editor = Reedline::create()
            .with_history(history)
            .with_hinter(Box::new(SQLHinter::new()))
            .with_highlighter(Box::new(SQLHighlighter))
            .with_validator(Box::new(SQLValidator));

        let prompt = SQLPrompt {};
        let mut scratch = String::with_capacity(1024);

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
                        let mut parts = buffer.splitn(2, ';');
                        let first = parts.next().unwrap();
                        scratch.push_str(first);

                        let second = parts.next();
                        if second.is_some() {
                            match self.execute(&scratch).await {
                                Ok(_) => {}
                                Err(e) => println!("Error: {e}"),
                            };
                            scratch.clear();
                        } else {
                            scratch.push(' ');
                        }
                    }
                },
                Ok(Signal::CtrlD) => break,
                Ok(Signal::CtrlC) => {
                    scratch.clear();
                }
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

    async fn execute(&mut self, text: &str) -> Result<()> {
        if is_client_cmd(text) {
            self.handle_client_cmd(text).await?;
            return Ok(());
        }

        const UNNAMED: String = String::new();

        let statements = parser::parse_sql(text)?;
        for stmt in statements {
            self.sess
                .prepare_statement(UNNAMED, Some(stmt), Vec::new())
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
                        self.opts.width,
                        self.opts.max_rows,
                        self.opts.max_columns,
                    )
                    .await?
                }
                other => println!("{}", other),
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
            ("\\max-columns", Some(val)) => self.opts.max_rows = Some(val.parse()?),
            ("\\open", Some(path)) => {
                if let Ok(url) = Url::parse(path) {
                    let new_opts = LocalClientOpts {
                        data_dir: None,
                        metastore_addr: None,
                        cloud_url: Some(url),
                        ..self.opts.clone()
                    };
                    let new_sess = LocalSession::connect(new_opts).await?;
                    *self = new_sess;
                } else {
                    let new_opts = LocalClientOpts {
                        data_dir: Some(PathBuf::from(path)),
                        metastore_addr: None,
                        cloud_url: None,
                        ..self.opts.clone()
                    };
                    let new_sess = LocalSession::connect(new_opts).await?;
                    *self = new_sess;
                }
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
    width: Option<usize>,
    max_rows: Option<usize>,
    max_columns: Option<usize>,
) -> Result<()> {
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
            let width = width.unwrap_or(term_width());
            let disp = pretty_format_batches(&batches, Some(width), max_rows, max_columns)?;
            println!("{disp}");
        }
        OutputMode::Csv => {
            let stdout = std::io::stdout();
            let buf = std::io::BufWriter::new(stdout);
            let mut writer = CsvWriterBuilder::new().has_headers(true).build(buf);
            for batch in batches {
                writer.write(&batch)?; // CSV writer flushes per write.
            }
        }
        OutputMode::Json => write_json::<JsonArrayNewLines>(&batches)?,
        OutputMode::Ndjson => write_json::<JsonLineDelimted>(&batches)?,
    }

    Ok(())
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

fn term_width() -> usize {
    crossterm::terminal::size()
        .map(|(width, _)| width as usize)
        .unwrap_or(80)
}
