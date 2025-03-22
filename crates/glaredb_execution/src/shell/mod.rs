//! Shell logic shared between the CLI and wasm shell.

pub mod highlighter;
pub mod lineedit;
pub mod raw;

mod debug;
mod vt100;

use std::io::{self, Write};

use glaredb_error::{DbError, Result};
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

#[derive(Debug)]
pub struct Shell<W: io::Write, P: PipelineExecutor, R: Runtime, T: RawModeTerm> {
    editor: LineEditor<W>,
    engine: Option<EngineWithConfig<P, R>>,
    term: T,
}

#[derive(Debug)]
struct EngineWithConfig<P: PipelineExecutor, R: Runtime> {
    engine: SingleUserEngine<P, R>,
    pending: Option<String>,
}

impl<W, P, R, T> Shell<W, P, R, T>
where
    W: io::Write,
    P: PipelineExecutor,
    R: Runtime,
    T: RawModeTerm,
{
    pub fn new(writer: W, term: T) -> Self {
        const PROMPT: &str = "glaredb> ";
        const CONTIN: &str = "     ... ";

        let editor = LineEditor::new(writer, PROMPT, CONTIN, TermSize { cols: 80 });
        Shell {
            editor,
            engine: None,
            term,
        }
    }

    pub fn attach(&mut self, engine: SingleUserEngine<P, R>, shell_msg: &str) -> Result<()> {
        self.engine = Some(EngineWithConfig {
            engine,
            pending: None,
        });

        let mut writer = RawTerminalWriter::new(self.editor.writer_mut());
        writeln!(
            writer,
            "{}{shell_msg}{}",
            vt100::MODE_BOLD,
            vt100::MODES_OFF
        )?;
        let version = env!("CARGO_PKG_VERSION");
        writeln!(writer, "Preview ({version})")?;

        Ok(())
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
            Signal::InputCompleted(query) => {
                let query = query.to_string();
                match self.engine.as_mut() {
                    Some(session) => {
                        session.pending = Some(query);
                    }
                    None => {
                        return Err(DbError::new("Attempted to run a query without a session"));
                    }
                }
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

        match self.engine.as_mut() {
            Some(engine) => {
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
                                None,
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
            None => Err(DbError::new(
                "Attempted to run query without attached engine",
            )),
        }
    }
}
