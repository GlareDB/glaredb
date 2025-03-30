use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use crossterm::event::{self, Event, KeyModifiers};
use ext_spark::SparkExtension;
use ext_tpch_gen::TpchGenExtension;
use glaredb_core::arrays::format::pretty::table::PrettyTable;
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::{PipelineExecutor, Runtime, TokioHandlerProvider};
use glaredb_core::shell::lineedit::{KeyEvent, TermSize};
use glaredb_core::shell::{RawModeTerm, Shell, ShellSignal};
use glaredb_error::Result;
use glaredb_rt_native::runtime::{NativeRuntime, ThreadedNativeExecutor};

#[derive(Parser)]
#[clap(name = "glaredb")]
struct Arguments {
    /// Execute file containing sql statements then exit.
    #[clap(short = 'f', long)]
    files: Vec<PathBuf>,
    /// Queries to execute.
    ///
    /// If omitted, and no files were given via the `files` argument, then an
    /// interactive session is started.
    #[clap(trailing_var_arg = true)]
    queries: Vec<String>,
}

/// Simple binary for quickly running arbitrary queries.
fn main() {
    let args = Arguments::parse();
    logutil::configure_global_logger(
        tracing::Level::ERROR,
        logutil::LogFormat::HumanReadable,
        io::stderr,
    );

    // Nested result. Outer result for the panic, inner is execution result.
    let result = std::panic::catch_unwind(|| {
        let executor = ThreadedNativeExecutor::try_new().unwrap();
        let runtime = NativeRuntime::with_default_tokio().unwrap();
        let tokio_handle = runtime
            .tokio_handle()
            .handle()
            .expect("tokio to be configured");

        // Note we do an explicit clone here to avoid dropping the tokio runtime
        // owned by the execution runtime inside the async context.
        let runtime_clone = runtime.clone();

        tokio_handle.block_on(async move { inner(args, executor, runtime_clone).await })
    });

    match result {
        Ok(Err(err)) => {
            // "Normal" error.
            println!("ERROR: {err}");
            std::process::exit(1);
        }
        Err(err) => {
            // Panic error.
            println!("PANIC: {err:?}");
            std::process::exit(2);
        }
        Ok(Ok(())) => (),
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

#[derive(Debug, Clone, Copy)]
struct CrosstermRawModeTerm;

impl RawModeTerm for CrosstermRawModeTerm {
    fn enable_raw_mode(&self) {
        let _ = crossterm::terminal::enable_raw_mode();
    }

    fn disable_raw_mode(&self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

async fn inner(
    args: Arguments,
    executor: impl PipelineExecutor,
    runtime: impl Runtime,
) -> Result<()> {
    let engine = SingleUserEngine::try_new(executor, runtime)?;
    engine.register_extension(SparkExtension)?;
    engine.register_extension(TpchGenExtension)?;

    let (cols, _rows) = crossterm::terminal::size()?;
    let mut stdout = BufWriter::new(std::io::stdout());

    if !args.files.is_empty() {
        // Files provided, read them and execute them in order.
        for path in args.files {
            let content = std::fs::read_to_string(path)?;

            let pending_queries = engine.session().query_many(&content)?;
            for pending in pending_queries {
                let mut q_res = pending.execute().await?;
                let batches = q_res.output.collect().await?;

                let table =
                    PrettyTable::try_new(&q_res.output_schema, &batches, cols as usize, None)?;
                writeln!(stdout, "{table}")?;

                stdout.flush()?;
            }
            stdout.flush()?;
        }

        return Ok(());
    }

    if !args.queries.is_empty() {
        // Queries provided directly, run and print them, and exit.
        for query in args.queries {
            let pending_queries = engine.session().query_many(&query)?;
            for pending in pending_queries {
                let mut query_res = pending.execute().await?;
                let batches = query_res.output.collect().await?;

                let table =
                    PrettyTable::try_new(&query_res.output_schema, &batches, cols as usize, None)?;

                writeln!(stdout, "{table}")?;
            }
            stdout.flush()?;
        }

        return Ok(());
    }

    // Otherwise continue on with interactive shell.

    let mut shell = Shell::start(
        stdout,
        CrosstermRawModeTerm,
        Some(TermSize {
            cols: cols as usize,
        }),
        engine,
        "GlareDB Shell",
    )?;

    let mut edit_guard = shell.edit_start()?;

    let inner_loop = || async move {
        loop {
            match event::read()? {
                Event::Key(event::KeyEvent {
                    code, modifiers, ..
                }) => {
                    let key = if modifiers.contains(KeyModifiers::CONTROL)
                        && matches!(code, event::KeyCode::Char('c'))
                    {
                        KeyEvent::CtrlC
                    } else {
                        from_crossterm_keycode(code)
                    };

                    match shell.consume_key(edit_guard, key)? {
                        ShellSignal::Continue(guard) => {
                            edit_guard = guard;
                        }
                        ShellSignal::ExecutePending(guard) => {
                            shell.execute_pending(guard).await?;
                            edit_guard = shell.edit_start()?;
                        }
                        ShellSignal::Exit => break,
                    }
                }
                Event::Resize(cols, _) => shell.set_size(TermSize {
                    cols: cols as usize,
                }),
                _event => (),
            }
        }
        Ok(())
    };

    inner_loop().await
}
