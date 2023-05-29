use crate::util::{ensure_spill_path, MetastoreMode};
use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use colored::Colorize;
use datafusion::arrow::csv::writer::WriterBuilder as CsvWriterBuilder;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::json::writer::{
    JsonFormat, LineDelimited as JsonLineDelimted, Writer as JsonWriter,
};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::FormatOptions;
use datafusion::arrow::util::pretty;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use pgrepr::format::Format;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sqlexec::engine::SessionLimits;
use sqlexec::engine::{Engine, TrackedSession};
use sqlexec::parser;
use sqlexec::session::ExecutionResult;
use std::fmt::Write as _;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use telemetry::Tracker;
use uuid::Uuid;

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

    /// Local file path to store database catalog (for a local persistent
    /// store).
    #[clap(short = 'f', long, value_parser)]
    pub local_file_path: Option<PathBuf>,

    /// Display output mode.
    #[arg(long, value_enum, default_value_t=OutputMode::Table)]
    pub mode: OutputMode,
}

impl LocalClientOpts {
    fn help_string() -> Result<String> {
        let pairs = [
            ("\\help", "Show this help text"),
            (
                "\\mode MODE",
                "Set the output mode [table, json, ndjson, csv]",
            ),
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

pub struct LocalSession {
    sess: TrackedSession,
    _engine: Engine, // Avoid dropping
    opts: LocalClientOpts,
}

impl LocalSession {
    pub async fn connect(opts: LocalClientOpts) -> Result<Self> {
        if opts.metastore_addr.is_some() && opts.local_file_path.is_some() {
            return Err(anyhow!(
                "Cannot specify both a metastore address and a local file path"
            ));
        }

        ensure_spill_path(opts.spill_path.as_ref())?;

        // Connect to metastore.
        let mode = MetastoreMode::new_from_options(
            opts.metastore_addr.clone(),
            opts.local_file_path.clone(),
            true,
        )?;
        let metastore_client = mode.into_client().await?;
        let tracker = Arc::new(Tracker::Nop);
        let engine = Engine::new(metastore_client, tracker, opts.spill_path.clone()).await?;

        Ok(LocalSession {
            sess: engine
                .new_session(
                    Uuid::nil(),
                    "glaredb".to_string(),
                    Uuid::nil(),
                    Uuid::nil(),
                    "glaredb".to_string(),
                    SessionLimits::default(),
                )
                .await?,
            _engine: engine,
            opts,
        })
    }

    pub async fn run_interactive(mut self) -> Result<()> {
        println!("GlareDB (v{})", env!("CARGO_PKG_VERSION"));
        let info = match (&self.opts.metastore_addr, &self.opts.local_file_path) {
            (Some(addr), None) => format!("Persisting catalog on remote metastore: {addr}"),
            (None, Some(path)) => format!("Persisting catalog at path: {}", path.display()),
            (None, None) => "Using in-memory catalog".to_string(),
            _ => unreachable!(),
        };
        println!("{}", info.bold());

        let mut rl = DefaultEditor::new()?;
        loop {
            let readline = rl.readline("> ");
            match readline {
                Ok(line) => {
                    if let Err(e) = self.execute(&line).await {
                        println!("ERROR: {e}");
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }

    pub async fn execute_one(mut self, query: &str) -> Result<()> {
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
                    print_stream(stream, self.opts.mode).await?
                }
                other => println!("{:?}", other),
            }
        }

        Ok(())
    }

    async fn handle_client_cmd(&mut self, text: &str) -> Result<()> {
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
            ("\\open", Some(path)) => {
                let new_opts = LocalClientOpts {
                    local_file_path: Some(PathBuf::from(path)),
                    metastore_addr: None,
                    ..self.opts.clone()
                };
                let new_sess = LocalSession::connect(new_opts).await?;
                println!("Created new session. New database path: {path}");
                *self = new_sess;
            }
            ("\\quit", None) => std::process::exit(0),
            (cmd, _) => return Err(anyhow!("Unable to handle client command: {cmd}")),
        }

        Ok(())
    }
}

static TABLE_FORMAT_OPTS: Lazy<FormatOptions> = Lazy::new(|| {
    FormatOptions::default()
        .with_display_error(false)
        .with_null("NULL")
});

async fn print_stream(stream: SendableRecordBatchStream, mode: OutputMode) -> Result<()> {
    let batches = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    fn write_json<F: JsonFormat>(batches: &[RecordBatch]) -> Result<()> {
        let stdout = std::io::stdout();
        let buf = std::io::BufWriter::new(stdout);
        let mut writer = JsonWriter::<_, F>::new(buf);
        writer.write_batches(batches)?;
        writer.finish()?;
        let mut buf = writer.into_inner();
        buf.flush()?;
        Ok(())
    }

    match mode {
        OutputMode::Table => {
            let disp = pretty::pretty_format_batches_with_options(&batches, &TABLE_FORMAT_OPTS)?;
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
    s.starts_with('\\')
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
