//! Shell logic shared between the CLI and wasm shell.

pub mod highlighter;
pub mod lineedit;
pub mod raw;

mod debug;
mod vt100;

use std::fmt::Debug;
use std::io::{self, Write};

use glaredb_error::{DbError, Result, ResultExt};
use lineedit::{KeyEvent, LineEditor, Signal, TermSize};
use raw::RawTerminalWriter;
use tracing::trace;

use crate::arrays::format::pretty::table::PrettyTable;
use crate::engine::single_user::SingleUserEngine;
use crate::runtime::{PipelineExecutor, Runtime};

/// Trait for enabling/disabling raw mode in a terminal.
pub trait RawModeTerm: Copy {
    fn enable_raw_mode(&self);
    fn disable_raw_mode(&self);
}

#[derive(Debug)]
pub struct RawModeGuard<T: RawModeTerm> {
    inner: T,
}

impl<T> RawModeGuard<T>
where
    T: RawModeTerm,
{
    fn enable_raw_mode(term: T) -> Self {
        term.enable_raw_mode();
        RawModeGuard { inner: term }
    }
}

impl<T> Drop for RawModeGuard<T>
where
    T: RawModeTerm,
{
    fn drop(&mut self) {
        self.inner.disable_raw_mode();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShellConfig {
    pub maxrows: usize,
}

impl Default for ShellConfig {
    fn default() -> Self {
        ShellConfig { maxrows: 50 }
    }
}

#[derive(Debug)]
pub enum ShellSignal<T: RawModeTerm> {
    /// Continue with reading the next input from the user.
    Continue(RawModeGuard<T>),
    /// Pending query needs to be executed.
    ///
    /// `execute_pending`, then `edit_start` to get a new guard to use.
    ExecutePending(RawModeGuard<T>),
    /// Exit the shell.
    Exit,
}

/// What to do after handling a dot command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DotSignal {
    /// Continue with normal editing.
    EditStart,
    /// Execute the pending query.
    ExecutePending,
}

trait DotCommand: Debug + Clone + Copy + Sized {
    const NAME: &str;
    const ARGS: &str;
    const HELP: &str;

    fn handle<W, P, R, T>(shell: &mut Shell<W, P, R, T>, args: &str) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineExecutor,
        R: Runtime,
        T: RawModeTerm;

    fn print_usage<W, P, R, T>(shell: &mut Shell<W, P, R, T>) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineExecutor,
        R: Runtime,
        T: RawModeTerm,
    {
        let mut writer = RawTerminalWriter::new(shell.editor.writer_mut());
        writeln!(writer, "Usage: .{} {}", Self::NAME, Self::ARGS)?;
        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandMaxRows;

impl DotCommand for DotCommandMaxRows {
    const NAME: &str = "maxrows";
    const ARGS: &str = "ROWS";
    const HELP: &str = "Set the maximum number of rows to display in the output";

    fn handle<W, P, R, T>(shell: &mut Shell<W, P, R, T>, args: &str) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineExecutor,
        R: Runtime,
        T: RawModeTerm,
    {
        let num = args
            .parse::<usize>()
            .context_fn(|| format!("Failed to parse '{args}' as a number"))?;
        shell.engine.config.maxrows = num;

        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandDatabases;

impl DotCommand for DotCommandDatabases {
    const NAME: &str = "databases";
    const ARGS: &str = "";
    const HELP: &str = "List attached databases";

    fn handle<W, P, R, T>(shell: &mut Shell<W, P, R, T>, args: &str) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineExecutor,
        R: Runtime,
        T: RawModeTerm,
    {
        if !args.is_empty() {
            return Self::print_usage(shell);
        }
        shell.engine.pending = Some("SHOW DATABASES".to_string());
        Ok(DotSignal::ExecutePending)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandTables;

impl DotCommand for DotCommandTables {
    const NAME: &str = "tables";
    const ARGS: &str = "";
    const HELP: &str = "List accessible tables";

    fn handle<W, P, R, T>(shell: &mut Shell<W, P, R, T>, args: &str) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineExecutor,
        R: Runtime,
        T: RawModeTerm,
    {
        if !args.is_empty() {
            return Self::print_usage(shell);
        }
        shell.engine.pending = Some("SHOW TABLES".to_string());
        Ok(DotSignal::ExecutePending)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandHelp;

impl DotCommand for DotCommandHelp {
    const NAME: &str = "help";
    const ARGS: &str = "";
    const HELP: &str = "Display this help text";

    fn handle<W, P, R, T>(shell: &mut Shell<W, P, R, T>, args: &str) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineExecutor,
        R: Runtime,
        T: RawModeTerm,
    {
        if !args.is_empty() {
            return Self::print_usage(shell);
        }

        const fn name_args_help<D: DotCommand>() -> (&'static str, &'static str, &'static str) {
            (D::NAME, D::ARGS, D::HELP)
        }

        // (name, args, help)
        const DOT_COMMAND_LINES: &[(&str, &str, &str)] = &[
            name_args_help::<DotCommandDatabases>(),
            name_args_help::<DotCommandHelp>(),
            name_args_help::<DotCommandMaxRows>(),
            name_args_help::<DotCommandTables>(),
        ];

        let mut writer = RawTerminalWriter::new(shell.editor.writer_mut());

        for (name, args, help) in DOT_COMMAND_LINES {
            let name_and_args = format!(".{name} {args}");
            writeln!(writer, "{:<20} {}", name_and_args, help)?
        }

        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug)]
pub struct Shell<W: io::Write, P: PipelineExecutor, R: Runtime, T: RawModeTerm> {
    editor: LineEditor<W>,
    engine: EngineWithConfig<P, R>,
    term: T,
}

#[derive(Debug)]
struct EngineWithConfig<P: PipelineExecutor, R: Runtime> {
    engine: SingleUserEngine<P, R>,
    pending: Option<String>,
    config: ShellConfig,
}

impl<W, P, R, T> Shell<W, P, R, T>
where
    W: io::Write,
    P: PipelineExecutor,
    R: Runtime,
    T: RawModeTerm,
{
    pub fn start(
        writer: W,
        term: T,
        initial_size: Option<TermSize>,
        engine: SingleUserEngine<P, R>,
        shell_msg: &str,
    ) -> Result<Self> {
        const PROMPT: &str = "glaredb> ";
        const CONTIN: &str = "     ... ";

        let initial_size = initial_size.unwrap_or(TermSize { cols: 80 });

        let editor = LineEditor::new(writer, PROMPT, CONTIN, initial_size);
        let mut shell = Shell {
            editor,
            engine: EngineWithConfig {
                engine,
                pending: None,
                config: ShellConfig::default(),
            },
            term,
        };

        let mut writer = RawTerminalWriter::new(shell.editor.writer_mut());
        writeln!(
            writer,
            "{}{shell_msg}{}",
            vt100::MODE_BOLD,
            vt100::MODES_OFF
        )?;
        let version = env!("CARGO_PKG_VERSION");
        writeln!(writer, "Preview ({version})")?;
        writeln!(
            writer,
            "Enter {}.help{} for usage hints.",
            vt100::MODE_BOLD,
            vt100::MODES_OFF
        )?;

        Ok(shell)
    }

    pub fn edit_start(&mut self) -> Result<RawModeGuard<T>> {
        trace!("edit start");
        let guard = RawModeGuard::enable_raw_mode(self.term);
        self.editor.edit_start()?;

        Ok(guard)
    }

    pub fn set_size(&mut self, size: TermSize) {
        self.editor.set_size(size);
    }

    // TODO: Should this allow signalling the end?
    pub fn consume_text(&mut self, text: &str) -> Result<()> {
        self.editor.consume_text(text)?;
        Ok(())
    }

    pub fn consume_key(&mut self, guard: RawModeGuard<T>, key: KeyEvent) -> Result<ShellSignal<T>> {
        match self.editor.consume_key(key)? {
            Signal::KeepEditing => Ok(ShellSignal::Continue(guard)),
            Signal::InputCompleted(input) => {
                if input.is_dot_command {
                    // TODO(minor): Try not not clone here.
                    let input = input.s.to_string();
                    return self.handle_dot_command(guard, &input);
                }

                let query = input.s.to_string();
                self.engine.pending = Some(query);
                Ok(ShellSignal::ExecutePending(guard))
            }
            Signal::Exit => Ok(ShellSignal::Exit),
        }
    }

    pub async fn execute_pending(&mut self, guard: RawModeGuard<T>) -> Result<()> {
        // Drop the guard here to exit raw mode before executing the query.
        //
        // This allows for nicer println debugging since the formatting won't be
        // all messed up. Also helps when we panic during execution... we don't
        // want to leave the terminal in raw mode.
        std::mem::drop(guard);

        let width = self.editor.get_size().cols;
        trace!(%width, "using editor reported width");

        let engine = &mut self.engine;
        let query = match engine.pending.take() {
            Some(query) => query,
            None => return Ok(()), // Nothing to execute.
        };
        // Using a raw writer here even in "cooked" mode is fine, since
        // all it does is replace '\n' with '\r\n' which works in either
        // mode.
        let mut writer = RawTerminalWriter::new(self.editor.writer_mut());

        match engine.engine.session().query_many(&query) {
            Ok(pending_queries) => {
                trace!("writing results");
                for pending in pending_queries {
                    let mut query_res = match pending.execute().await {
                        Ok(table) => table,
                        Err(e) => {
                            writeln!(writer, "{e}")?;
                            break;
                        }
                    };

                    let batches = match query_res.output.collect().await {
                        Ok(batches) => batches,
                        Err(e) => {
                            writeln!(writer, "{e}")?;
                            break;
                        }
                    };

                    match PrettyTable::try_new(
                        &query_res.output_schema,
                        &batches,
                        width,
                        Some(engine.config.maxrows),
                    ) {
                        Ok(table) => {
                            writeln!(writer, "{table}")?;
                        }
                        Err(e) => {
                            writeln!(writer, "{e}")?;
                            break;
                        }
                    }

                    // Flush after every table write so that user
                    // doesn't have to wait for all queries to complete
                    // before getting output (xterm.js).
                    writer.flush()?;
                }
            }
            Err(e) => {
                // We're not returning the error here since it's related
                // to the user input. We want to show the error to the
                // user.
                writeln!(writer, "{e}")?;
            }
        }

        // Flush here too idk.
        writer.flush()?;

        Ok(())
    }

    /// Handle a dot command.
    fn handle_dot_command(
        &mut self,
        guard: RawModeGuard<T>,
        command: &str,
    ) -> Result<ShellSignal<T>> {
        match self.handle_dot_command_inner(command) {
            Ok(sig) => match sig {
                DotSignal::EditStart => {
                    self.editor.edit_start()?;
                    Ok(ShellSignal::Continue(guard))
                }
                DotSignal::ExecutePending => Ok(ShellSignal::ExecutePending(guard)),
            },
            Err(e) => {
                // User input error, print it out instead of returning it.
                let mut writer = RawTerminalWriter::new(self.editor.writer_mut());
                writeln!(writer, "{e}")?;

                // Begin editing again.
                self.editor.edit_start()?;
                Ok(ShellSignal::Continue(guard))
            }
        }
    }

    fn handle_dot_command_inner(&mut self, command: &str) -> Result<DotSignal> {
        let (first, rest) = match command.split(' ').next() {
            Some(first) => (first, command.trim_start_matches(first).trim()),
            None => return Err(DbError::new("Empty dot command")),
        };

        let name = first.trim_start_matches('.');

        match name {
            DotCommandDatabases::NAME => DotCommandDatabases::handle(self, rest),
            DotCommandTables::NAME => DotCommandTables::handle(self, rest),
            DotCommandHelp::NAME => DotCommandHelp::handle(self, rest),
            DotCommandMaxRows::NAME => DotCommandMaxRows::handle(self, rest),
            other => Err(DbError::new(format!("Unknown dot command: '{other}'"))),
        }
    }
}
