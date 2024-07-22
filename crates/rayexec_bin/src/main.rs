use std::io::{BufWriter, Write};
use std::sync::Arc;

use clap::Parser;
use crossterm::event::{self, Event, KeyModifiers};
use rayexec_csv::CsvDataSource;
use rayexec_delta::DeltaDataSource;
use rayexec_error::Result;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_execution::runtime::ExecutionRuntime;
use rayexec_parquet::ParquetDataSource;
use rayexec_postgres::PostgresDataSource;
use rayexec_rt_native::runtime::ThreadedExecutionRuntime;
use rayexec_shell::lineedit::KeyEvent;
use rayexec_shell::session::SingleUserEngine;
use rayexec_shell::shell::{Shell, ShellSignal};

#[derive(Parser)]
#[clap(name = "rayexec_bin")]
struct Arguments {
    /// Queries to execute.
    ///
    /// If omitted, an interactive session will be automatically started.
    #[clap(trailing_var_arg = true)]
    queries: Vec<String>,
}

/// Simple binary for quickly running arbitrary queries.
fn main() {
    let args = Arguments::parse();
    logutil::configure_global_logger(tracing::Level::ERROR);

    let runtime = Arc::new(
        ThreadedExecutionRuntime::try_new()
            .unwrap()
            .with_default_tokio()
            .unwrap(),
    );
    let tokio_handle = runtime.tokio_handle().expect("tokio to be configured");

    // Note we do an explicit clone here to avoid dropping the tokio runtime
    // owned by the execution runtime inside the async context.
    let runtime_clone = runtime.clone();
    let result = tokio_handle.block_on(async move { inner(args, runtime_clone).await });

    if let Err(e) = result {
        println!("ERROR: {e}");
        std::process::exit(1);
    }
}

fn from_crossterm_keycode(code: crossterm::event::KeyCode) -> KeyEvent {
    match code {
        crossterm::event::KeyCode::Backspace => KeyEvent::Backspace,
        crossterm::event::KeyCode::Enter => KeyEvent::Enter,
        crossterm::event::KeyCode::Left => KeyEvent::Left,
        crossterm::event::KeyCode::Right => KeyEvent::Right,
        crossterm::event::KeyCode::Up => KeyEvent::Up,
        crossterm::event::KeyCode::Down => KeyEvent::Down,
        crossterm::event::KeyCode::Home => KeyEvent::Home,
        crossterm::event::KeyCode::End => KeyEvent::End,
        crossterm::event::KeyCode::Tab => KeyEvent::Tab,
        crossterm::event::KeyCode::BackTab => KeyEvent::BackTab,
        crossterm::event::KeyCode::Delete => KeyEvent::Delete,
        crossterm::event::KeyCode::Insert => KeyEvent::Insert,
        crossterm::event::KeyCode::Char(c) => KeyEvent::Char(c),
        _ => KeyEvent::Unknown,
    }
}

async fn inner(args: Arguments, runtime: Arc<dyn ExecutionRuntime>) -> Result<()> {
    let registry = DataSourceRegistry::default()
        .with_datasource("memory", Box::new(MemoryDataSource))?
        .with_datasource("postgres", Box::new(PostgresDataSource))?
        .with_datasource("delta", Box::new(DeltaDataSource))?
        .with_datasource("parquet", Box::new(ParquetDataSource))?
        .with_datasource("csv", Box::new(CsvDataSource))?;
    let engine = SingleUserEngine::new_with_runtime(runtime, registry)?;

    let (cols, _rows) = crossterm::terminal::size()?;
    let mut stdout = BufWriter::new(std::io::stdout());

    if !args.queries.is_empty() {
        // Queries provided directly, run and print them, and exit.

        // TODO: Check if file, read file...

        for query in args.queries {
            let tables = engine.sql(&query).await?;
            for table in tables {
                writeln!(stdout, "{}", table.pretty_table(cols as usize, None)?)?;
            }
            stdout.flush()?;
        }

        return Ok(());
    }

    // Otherwise continue on with interactive shell.

    crossterm::terminal::enable_raw_mode()?;

    let shell = Shell::new(stdout);
    shell.set_cols(cols as usize);
    shell.attach(engine, "Rayexec Shell")?;

    let inner_loop = || async move {
        loop {
            match event::read()? {
                Event::Key(event::KeyEvent {
                    code, modifiers, ..
                }) => {
                    let key = if modifiers.contains(KeyModifiers::CONTROL) {
                        match code {
                            event::KeyCode::Char('c') => KeyEvent::CtrlC,
                            _ => KeyEvent::Unknown,
                        }
                    } else {
                        from_crossterm_keycode(code)
                    };

                    match shell.consume_key(key)? {
                        ShellSignal::Continue => (),
                        ShellSignal::ExecutePending => shell.execute_pending().await?,
                        ShellSignal::Exit => break,
                    }
                }
                Event::Resize(cols, _) => shell.set_cols(cols as usize),
                _event => (),
            }
        }
        Ok(())
    };

    let result = inner_loop().await;
    crossterm::terminal::disable_raw_mode()?;

    result
}
