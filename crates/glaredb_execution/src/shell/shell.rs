use std::cell::RefCell;
use std::io::{self, Write};

use glaredb_error::{RayexecError, Result};
use tracing::trace;

use super::lineedit::{KeyEvent, LineEditor, Signal, TermSize};
use super::raw::RawTerminalWriter;
use super::vt100::{MODES_OFF, MODE_BOLD};
use crate::arrays::format::pretty::table::PrettyTable;
use crate::engine::single_user::SingleUserEngine;
use crate::runtime::{PipelineExecutor, Runtime};

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
    ExecutePending(RawModeGuard<T>),
    /// Exit the shell.
    Exit,
}

#[derive(Debug)]
pub struct Shell<W: io::Write, P: PipelineExecutor, R: Runtime, T: RawModeTerm> {
    editor: RefCell<LineEditor<W>>,
    engine: RefCell<Option<EngineWithConfig<P, R>>>,
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
            editor: RefCell::new(editor),
            engine: RefCell::new(None),
            term,
        }
    }

    pub fn attach(&self, engine: SingleUserEngine<P, R>, shell_msg: &str) -> Result<()> {
        let mut current = self.engine.borrow_mut();
        *current = Some(EngineWithConfig {
            engine,
            pending: None,
        });

        let mut editor = self.editor.borrow_mut();
        let mut writer = RawTerminalWriter::new(editor.writer_mut());
        writeln!(writer, "{}{shell_msg}{}", MODE_BOLD, MODES_OFF)?;
        let version = env!("CARGO_PKG_VERSION");
        writeln!(writer, "Preview ({version}) - There will be bugs!")?;

        Ok(())
    }

    pub fn edit_start(&self) -> Result<RawModeGuard<T>> {
        let guard = RawModeGuard::enable_raw_mode(self.term);
        let mut editor = self.editor.borrow_mut();
        editor.edit_start()?;

        Ok(guard)
    }

    pub fn set_size(&self, size: TermSize) {
        let mut editor = self.editor.borrow_mut();
        editor.set_size(size);
    }

    // TODO: Should this allow signalling the end?
    pub fn consume_text(&self, text: &str) -> Result<()> {
        let mut editor = self.editor.borrow_mut();
        editor.consume_text(text)?;
        Ok(())
    }

    pub fn consume_key(&self, guard: RawModeGuard<T>, key: KeyEvent) -> Result<ShellSignal<T>> {
        let mut editor = self.editor.borrow_mut();

        match editor.consume_key(key)? {
            Signal::KeepEditing => Ok(ShellSignal::Continue(guard)),
            Signal::InputCompleted(query) => {
                let query = query.to_string();
                let mut session = self.engine.borrow_mut();
                match session.as_mut() {
                    Some(session) => {
                        session.pending = Some(query);
                    }
                    None => {
                        return Err(RayexecError::new(
                            "Attempted to run a query without a session",
                        ))
                    }
                }
                Ok(ShellSignal::ExecutePending(guard))
            }
            Signal::Exit => Ok(ShellSignal::Exit),
        }
    }

    // TODO: The refcell stuff here is definitely prone to erroring. I'd prefer
    // some sort of message passing approach.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn execute_pending(&self, guard: RawModeGuard<T>) -> Result<()> {
        // Drop the guard, we want to leave raw mode so that printing, etc works
        // as normal during execution (aka debug printing).
        std::mem::drop(guard);

        let mut editor = self.editor.borrow_mut();
        let width = editor.get_size().cols;

        let mut engine = self.engine.borrow_mut();
        match engine.as_mut() {
            Some(engine) => {
                let query = match engine.pending.take() {
                    Some(query) => query,
                    None => return Ok(()), // Nothing to execute.
                };
                let mut writer = RawTerminalWriter::new(editor.writer_mut());
                writer.write_all(b"\n")?;

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
                        }
                    }
                    Err(e) => {
                        // We're not returning the error here since it's related
                        // to the user input. We want to show the error to the
                        // user.
                        writeln!(writer, "{e}")?;
                    }
                }

                Ok(())
            }
            None => Err(RayexecError::new(
                "Attempted to run query without attached engine",
            )),
        }
    }
}
