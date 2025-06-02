use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use crossterm::event::{self, Event, KeyModifiers};
use glaredb_core::engine::single_user::SingleUserEngine;
use glaredb_core::runtime::pipeline::PipelineRuntime;
use glaredb_core::runtime::system::SystemRuntime;
use glaredb_core::shell::lineedit::{KeyEvent, TermSize, UserInput};
use glaredb_core::shell::{InteractiveShell, RawModeTerm, Shell, ShellSignal};
use glaredb_error::Result;
use glaredb_rt_native::runtime::{
    NativeSystemRuntime,
    ThreadedNativeExecutor,
    new_tokio_runtime_for_io,
};

#[derive(Parser)]
#[clap(name = "glaredb")]
#[clap(version)]
struct Arguments {
    /// Execute a file containing SQL statements and continue.
    #[clap(long)]
    init: Option<PathBuf>,
    /// Execute a file containing SQL statements then exit.
    #[clap(short = 'f', long)]
    files: Vec<PathBuf>,
    /// Execute SQL commands then exit.
    #[clap(short = 'c', long)]
    commands: Vec<String>,
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
        let tokio_rt = new_tokio_runtime_for_io()?;

        let executor = ThreadedNativeExecutor::try_new().unwrap();
        let runtime = NativeSystemRuntime::new(tokio_rt.handle().clone());

        tokio_rt.block_on(async move { inner(args, executor, runtime).await })
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
    executor: impl PipelineRuntime,
    runtime: impl SystemRuntime,
) -> Result<()> {
    let engine = SingleUserEngine::try_new(executor, runtime.clone())?;
    ext_default::register_all(&engine.engine)?;

    let mut stdout = BufWriter::new(std::io::stdout());

    // Try our best to get the size. We query the size even for non-interactive
    // session in order to pretty print the table with a reasonable width.
    //
    // However we should still allow moving forward even if not in a proper tty.
    let (cols, _rows) = crossterm::terminal::size().unwrap_or((80, 24));
    let cols = cols as usize;

    let mut shell = Shell::new(engine);

    if let Some(init) = args.init {
        let content = std::fs::read_to_string(init)?;
        shell
            .handle_input_non_interactive(UserInput::new(&content), cols, &mut stdout)
            .await?;
        stdout.flush()?;
        // Continue on...
    }

    if !args.files.is_empty() {
        for path in args.files {
            let content = std::fs::read_to_string(path)?;
            shell
                .handle_input_non_interactive(UserInput::new(&content), cols, &mut stdout)
                .await?;
            stdout.flush()?;
        }
        return Ok(());
    }

    if !args.commands.is_empty() {
        for command in args.commands {
            shell
                .handle_input_non_interactive(UserInput::new(&command), cols, &mut stdout)
                .await?;
            stdout.flush()?;
        }
        return Ok(());
    }

    // Otherwise continue on with interactive shell.

    let mut shell = InteractiveShell::start(
        stdout,
        CrosstermRawModeTerm,
        Some(TermSize { cols }),
        shell,
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
