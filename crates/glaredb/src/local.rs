use crate::util::{ensure_spill_path, MetastoreMode};
use anyhow::Result;
use datafusion::arrow::util::display::FormatOptions;
use datafusion::arrow::util::pretty;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use once_cell::sync::Lazy;
use pgrepr::format::Format;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use sqlexec::engine::Engine;
use sqlexec::parser;
use sqlexec::session::ExecutionResult;
use sqlexec::session::Session;
use std::path::PathBuf;
use std::sync::Arc;
use telemetry::Tracker;
use uuid::Uuid;

/// Run a single session locally.
pub struct LocalEngine {
    engine: Engine,
}

impl LocalEngine {
    pub async fn connect(
        metastore_addr: Option<String>,
        local_file_path: Option<PathBuf>,
        spill_path: Option<PathBuf>,
    ) -> Result<Self> {
        ensure_spill_path(spill_path.as_ref())?;

        // Connect to metastore.
        let mode = MetastoreMode::new_from_options(metastore_addr, local_file_path, true)?;
        let metastore_client = mode.into_client().await?;

        let tracker = Arc::new(Tracker::Nop);

        let engine = Engine::new(metastore_client, tracker, spill_path).await?;

        Ok(LocalEngine { engine })
    }

    pub async fn run(self) -> Result<()> {
        println!("GlareDB ({})", env!("CARGO_PKG_VERSION"));

        let mut session = self
            .engine
            .new_session(
                Uuid::nil(),
                "glaredb".to_string(),
                Uuid::nil(),
                Uuid::nil(),
                "glaredb".to_string(),
                0,
                0,
            )
            .await?;

        let mut rl = DefaultEditor::new()?;
        loop {
            let readline = rl.readline("> ");
            match readline {
                Ok(line) => {
                    if let Err(e) = execute_line(&mut session, &line).await {
                        println!("ERROR: {e}");
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    break;
                }
                Err(ReadlineError::Eof) => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }
        Ok(())
    }
}

static FORMAT_OPTS: Lazy<FormatOptions> = Lazy::new(|| {
    FormatOptions::default()
        .with_display_error(false)
        .with_null("NULL")
});

const UNNAMED: String = String::new();

/// Execute a line against the session.
async fn execute_line(sess: &mut Session, line: &str) -> Result<()> {
    let statements = parser::parse_sql(line)?;

    for stmt in statements {
        sess.prepare_statement(UNNAMED, Some(stmt), Vec::new())
            .await?;
        let prepared = sess.get_prepared_statement(&UNNAMED)?;
        let num_fields = prepared.output_fields().map(|f| f.len()).unwrap_or(0);
        sess.bind_statement(
            UNNAMED,
            &UNNAMED,
            Vec::new(),
            vec![Format::Text; num_fields],
        )?;
        let result = sess.execute_portal(&UNNAMED, 0).await?;

        match result {
            ExecutionResult::Query { stream, .. } => print_stream(stream).await?,
            ExecutionResult::ShowVariable { stream } => print_stream(stream).await?,
            other => println!("{:?}", other),
        }
    }

    Ok(())
}

async fn print_stream(stream: SendableRecordBatchStream) -> Result<()> {
    let batches = stream
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;

    let disp = pretty::pretty_format_batches_with_options(&batches, &FORMAT_OPTS)?;
    println!("{disp}");

    Ok(())
}
