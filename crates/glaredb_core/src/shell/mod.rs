//! Shell logic shared between the CLI and wasm shell.

pub mod highlighter;
pub mod lineedit;
pub mod raw;

mod code_point_string;
mod debug;
mod vt100;

use std::fmt::Debug;
use std::io::{self, Write};

use glaredb_error::{DbError, Result, ResultExt};
use lineedit::{KeyEvent, LineEditor, Signal, TermSize, UserInput};
use raw::RawTerminalWriter;
use tracing::trace;

use crate::arrays::format::pretty::components::{
    ASCII_COMPONENTS,
    PRETTY_COMPONENTS,
    TableComponents,
};
use crate::arrays::format::pretty::table::PrettyTable;
use crate::engine::single_user::SingleUserEngine;
use crate::runtime::pipeline::PipelineRuntime;
use crate::runtime::system::SystemRuntime;
use crate::runtime::time::RuntimeInstant;

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
    pub timer: bool,
    pub table_components: &'static TableComponents,
}

impl Default for ShellConfig {
    fn default() -> Self {
        ShellConfig {
            maxrows: 50,
            timer: false,
            table_components: PRETTY_COMPONENTS,
        }
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

    fn handle<W, P, R>(shell: &mut Shell<P, R>, args: &str, writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime;

    fn print_usage<W>(writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
    {
        let mut writer = RawTerminalWriter::new(writer);
        writeln!(writer, "Usage: .{} {}", Self::NAME, Self::ARGS)?;
        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandTimer;

impl DotCommand for DotCommandTimer {
    const NAME: &str = "timer";
    const ARGS: &str = "on|off";
    const HELP: &str = "Display time taken to execute a query";

    fn handle<W, P, R>(shell: &mut Shell<P, R>, args: &str, _writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime,
    {
        match args {
            "on" => shell.config.timer = true,
            "off" => shell.config.timer = false,
            _ => return Err(DbError::new("Expected 'on' or 'off' as arguments")),
        }

        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandMaxRows;

impl DotCommand for DotCommandMaxRows {
    const NAME: &str = "maxrows";
    const ARGS: &str = "ROWS";
    const HELP: &str = "Set the maximum number of rows to display in the output";

    fn handle<W, P, R>(shell: &mut Shell<P, R>, args: &str, _writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime,
    {
        let num = args
            .parse::<usize>()
            .context_fn(|| format!("Failed to parse '{args}' as a number"))?;
        shell.config.maxrows = num;

        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandDatabases;

impl DotCommand for DotCommandDatabases {
    const NAME: &str = "databases";
    const ARGS: &str = "";
    const HELP: &str = "List attached databases";

    fn handle<W, P, R>(shell: &mut Shell<P, R>, args: &str, writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime,
    {
        if !args.is_empty() {
            return Self::print_usage(writer);
        }
        shell.pending = Some("SHOW DATABASES".to_string());
        Ok(DotSignal::ExecutePending)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandBox;

impl DotCommand for DotCommandBox {
    const NAME: &str = "box";
    const ARGS: &str = "pretty|ascii";
    const HELP: &str = "Characters to use for box drawing";

    fn handle<W, P, R>(shell: &mut Shell<P, R>, args: &str, writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime,
    {
        match args {
            "pretty" => shell.config.table_components = PRETTY_COMPONENTS,
            "ascii" => shell.config.table_components = ASCII_COMPONENTS,
            _ => return Self::print_usage(writer),
        }
        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandTables;

impl DotCommand for DotCommandTables {
    const NAME: &str = "tables";
    const ARGS: &str = "";
    const HELP: &str = "List accessible tables";

    fn handle<W, P, R>(shell: &mut Shell<P, R>, args: &str, writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime,
    {
        if !args.is_empty() {
            return Self::print_usage(writer);
        }
        shell.pending = Some("SHOW TABLES".to_string());
        Ok(DotSignal::ExecutePending)
    }
}

#[derive(Debug, Clone, Copy)]
struct DotCommandHelp;

impl DotCommand for DotCommandHelp {
    const NAME: &str = "help";
    const ARGS: &str = "";
    const HELP: &str = "Display this help text";

    fn handle<W, P, R>(_shell: &mut Shell<P, R>, args: &str, writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
        P: PipelineRuntime,
        R: SystemRuntime,
    {
        if !args.is_empty() {
            return Self::print_usage(writer);
        }

        const fn name_args_help<D: DotCommand>() -> (&'static str, &'static str, &'static str) {
            (D::NAME, D::ARGS, D::HELP)
        }

        // (name, args, help)
        const DOT_COMMAND_LINES: &[(&str, &str, &str)] = &[
            name_args_help::<DotCommandBox>(),
            name_args_help::<DotCommandDatabases>(),
            name_args_help::<DotCommandHelp>(),
            name_args_help::<DotCommandMaxRows>(),
            name_args_help::<DotCommandTables>(),
            name_args_help::<DotCommandTimer>(),
        ];

        let mut writer = RawTerminalWriter::new(writer);

        writeln!(
            writer,
            "{}Dot commands:{}",
            vt100::MODE_BOLD,
            vt100::MODES_OFF
        )?;
        for (name, args, help) in DOT_COMMAND_LINES {
            let name_and_args = format!(".{name} {args}");
            writeln!(writer, "{name_and_args:<20} {help}")?
        }
        writeln!(
            writer,
            "{}Online docs:{} <https://glaredb.com/docs>",
            vt100::MODE_BOLD,
            vt100::MODES_OFF
        )?;

        Ok(DotSignal::EditStart)
    }
}

#[derive(Debug)]
pub struct InteractiveShell<W: io::Write, P: PipelineRuntime, R: SystemRuntime, T: RawModeTerm> {
    editor: LineEditor<W>,
    shell: Shell<P, R>,
    term: T,
}

impl<W, P, R, T> InteractiveShell<W, P, R, T>
where
    W: io::Write,
    P: PipelineRuntime,
    R: SystemRuntime,
    T: RawModeTerm,
{
    pub fn start(
        writer: W,
        term: T,
        initial_size: Option<TermSize>,
        shell: Shell<P, R>,
        shell_msg: &str,
    ) -> Result<Self> {
        const PROMPT: &str = "glaredb> ";
        const CONTIN: &str = "     ... ";

        let initial_size = initial_size.unwrap_or(TermSize { cols: 80 });

        let editor = LineEditor::new(writer, PROMPT, CONTIN, initial_size);
        let mut shell = InteractiveShell {
            editor,
            shell,
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
        writeln!(writer, "v{version}")?;
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
                self.shell.pending = Some(query);
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

        self.shell
            .execute_pending(width, self.editor.writer_mut())
            .await?;

        Ok(())
    }

    /// Handle a dot command.
    fn handle_dot_command(
        &mut self,
        guard: RawModeGuard<T>,
        command: &str,
    ) -> Result<ShellSignal<T>> {
        match self
            .shell
            .handle_dot_command(command, self.editor.writer_mut())
        {
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
}

#[derive(Debug)]
pub struct Shell<P: PipelineRuntime, R: SystemRuntime> {
    engine: SingleUserEngine<P, R>,
    pending: Option<String>,
    config: ShellConfig,
}

impl<P, R> Shell<P, R>
where
    P: PipelineRuntime,
    R: SystemRuntime,
{
    pub fn new(engine: SingleUserEngine<P, R>) -> Self {
        Shell {
            engine,
            pending: None,
            config: ShellConfig::default(),
        }
    }

    pub async fn handle_input_non_interactive<W>(
        &mut self,
        input: UserInput<'_>,
        width: usize,
        writer: &mut W,
    ) -> Result<()>
    where
        W: io::Write,
    {
        if input.is_dot_command {
            self.handle_dot_command(input.s, writer)?;
        } else {
            self.pending = Some(input.s.to_string());
        }

        self.execute_pending(width, writer).await?;

        Ok(())
    }

    async fn execute_pending<W>(&mut self, width: usize, writer: &mut W) -> Result<()>
    where
        W: io::Write,
    {
        let timer = self.config.timer;
        let query = match self.pending.take() {
            Some(query) => query,
            None => return Ok(()), // Nothing to execute.
        };
        // Using a raw writer here even in "cooked" mode is fine, since
        // all it does is replace '\n' with '\r\n' which works in either
        // mode.
        let mut writer = RawTerminalWriter::new(writer);

        match self.engine.session().query_many(&query) {
            Ok(pending_queries) => {
                trace!("writing results");
                for pending in pending_queries {
                    let start = if timer { Some(Self::now()) } else { None };

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
                        Some(self.config.maxrows),
                        self.config.table_components,
                    ) {
                        Ok(table) => {
                            writeln!(writer, "{table}")?;
                            if let Some(start) = start {
                                let exec_dur = Self::now().duration_since(start).as_secs_f64();
                                writeln!(writer, "Execution duration (s): {exec_dur:.5}")?;
                            }
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

    fn handle_dot_command<W>(&mut self, command: &str, writer: &mut W) -> Result<DotSignal>
    where
        W: io::Write,
    {
        let (first, rest) = match command.split(' ').next() {
            Some(first) => (first, command.trim_start_matches(first).trim()),
            None => return Err(DbError::new("Empty dot command")),
        };

        let name = first.trim_start_matches('.');

        match name {
            DotCommandDatabases::NAME => DotCommandDatabases::handle(self, rest, writer),
            DotCommandTables::NAME => DotCommandTables::handle(self, rest, writer),
            DotCommandHelp::NAME => DotCommandHelp::handle(self, rest, writer),
            DotCommandMaxRows::NAME => DotCommandMaxRows::handle(self, rest, writer),
            DotCommandTimer::NAME => DotCommandTimer::handle(self, rest, writer),
            DotCommandBox::NAME => DotCommandBox::handle(self, rest, writer),
            other => Err(DbError::new(format!("Unknown dot command: '{other}'"))),
        }
    }

    fn now() -> R::Instant {
        <R::Instant as RuntimeInstant>::now()
    }
}
