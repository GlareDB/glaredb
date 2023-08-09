use crate::highlighter::SQLHighlighter;
use crate::prompt::SQLPrompt;
use crate::util::MetastoreClientMode;
use anyhow::{anyhow, Result};
use arrow_util::pretty::pretty_format_batches;
use clap::{Parser, ValueEnum};
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
use sqlexec::remote::client::{AuthenticatedExecutionServiceClient, ProxyAuthParamsAndDst};
use sqlexec::session::ExecutionResult;
use std::env;
use std::fmt::Write as _;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use telemetry::Tracker;
use tracing::error;
use url::Url;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum OutputMode {
    Table,
    Json,
    Ndjson,
    Csv,
}

#[derive(Debug, Clone, Parser)]
pub struct LocalClientOpts {
    /// Address to the Metastore.
    ///
    /// If not provided, an in-process metastore will be started.
    #[clap(short, long, value_parser)]
    pub metastore_addr: Option<String>,

    /// Path to spill temporary files to.
    #[clap(long, value_parser)]
    pub spill_path: Option<PathBuf>,

    /// Optional file path for persisting data.
    ///
    /// Catalog data and user data will be stored in this directory.
    #[clap(short = 'f', long, value_parser)]
    pub data_dir: Option<PathBuf>,

    /// URL for connecting to a GlareDB Cloud deployment.
    #[clap(short = 'c', long, value_parser)]
    pub cloud_url: Option<Url>,

    /// Display output mode.
    #[arg(long, value_enum, default_value_t=OutputMode::Table)]
    pub mode: OutputMode,

    /// Max width for tables to display.
    #[clap(long)]
    pub width: Option<usize>,

    /// Max number of rows to display.
    #[arg(long)]
    pub max_rows: Option<usize>,

    /// Max number of columns to display.
    #[arg(long)]
    pub max_columns: Option<usize>,
}

impl LocalClientOpts {
    fn help_string() -> Result<String> {
        let pairs = [
            ("\\help", "Show this help text"),
            (
                "\\mode MODE",
                "Set the output mode [table, json, ndjson, csv]",
            ),
            ("\\max-rows NUM", "Max number of rows to display"),
            ("\\max-columns NUM", "Max number of columns to display"),
            ("\\open PATH", "Open a database at the given path"),
            ("\\quit", "Quit this session"),
        ];

        let mut buf = String::new();
        for (cmd, help) in pairs {
            writeln!(&mut buf, "{cmd: <15} {help}")?;
        }

        Ok(buf)
    }
}

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
            let params_and_dst = ProxyAuthParamsAndDst::try_from_url(url)?;
            let exec_client = AuthenticatedExecutionServiceClient::connect_with_proxy_auth_params(
                params_and_dst.dst.to_string(),
                params_and_dst.params,
            )
            .await?;
            engine
                .new_session_with_remote_connection(SessionVars::default(), exec_client)
                .await?
        } else {
            engine
                .new_session(SessionVars::default(), SessionStorageConfig::default())
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

        // Try to shutdown the engine gracefully.
        if let Err(err) = self.engine.shutdown().await {
            error!(%err, "unable to shutdown the engine gracefully");
        }

        result
    }

    async fn run_interactive(&mut self) -> Result<()> {
        let history = Box::new(
            FileBackedHistory::with_file(100, get_history_path())
                .expect("Error configuring history with file"),
        );

        let mut line_editor = Reedline::create().with_history(history);

        let sql_highlighter = SQLHighlighter {};
        line_editor = line_editor.with_highlighter(Box::new(sql_highlighter));

        println!("GlareDB (v{})", env!("CARGO_PKG_VERSION"));
        println!("Type \\help for help.");

        let info = match (&self.opts.metastore_addr, &self.opts.data_dir) {
            (Some(addr), None) => format!("Persisting catalog on remote metastore: {addr}"), // TODO: Should we continue to allow this?
            (None, Some(path)) => format!("Persisting database at path: {}", path.display()),
            (None, None) => "Using in-memory catalog".to_string(),
            _ => unreachable!(),
        };
        println!("{}", info.bold());
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
            let result = self.sess.execute_portal(&UNNAMED, 0).await?;

            match result {
                ExecutionResult::Query { stream, .. }
                | ExecutionResult::ShowVariable { stream } => {
                    print_stream(
                        stream,
                        self.opts.mode,
                        self.opts.width,
                        self.opts.max_rows,
                        self.opts.max_columns,
                    )
                    .await?
                }
                other => println!("{:?}", other),
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

fn is_client_cmd(s: &str) -> bool {
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
