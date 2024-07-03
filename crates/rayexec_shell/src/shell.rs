use std::{
    cell::RefCell,
    io::{self, Write},
};

use futures::StreamExt;
use rayexec_bullet::format::pretty::table::PrettyTable;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::engine::{result::ExecutionResult, session::Session};
use tracing::trace;

use crate::{
    lineedit::{KeyEvent, LineEditor, Signal},
    vt100::{MODES_OFF, MODE_BOLD},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShellSignal {
    /// Continue with reading the next input from the user.
    Continue,

    /// Pending query needs to be executed.
    ExecutePending,

    /// Exit the shell.
    Exit,
}

#[derive(Debug)]
pub struct Shell<W: io::Write> {
    editor: RefCell<LineEditor<W>>,
    session: RefCell<Option<SessionWithConfig>>,
}

#[derive(Debug)]
struct SessionWithConfig {
    session: Session,
    pending: Option<String>,
}

impl<W: io::Write> Shell<W> {
    pub fn new(writer: W) -> Self {
        let editor = LineEditor::new(writer, ">> ", 80);
        Shell {
            editor: RefCell::new(editor),
            session: RefCell::new(None),
        }
    }

    pub fn attach(&self, session: Session, shell_msg: &str) -> Result<()> {
        let mut current = self.session.borrow_mut();
        *current = Some(SessionWithConfig {
            session,
            pending: None,
        });

        let mut editor = self.editor.borrow_mut();
        writeln!(editor.raw_writer(), "{}{shell_msg}{}", MODE_BOLD, MODES_OFF)?;
        let version = env!("CARGO_PKG_VERSION");
        writeln!(
            editor.raw_writer(),
            "Preview ({version}) - There will be bugs!"
        )?;
        editor.raw_writer().write_all(&[b'\n'])?;

        editor.edit_start()?;

        Ok(())
    }

    pub fn set_cols(&self, cols: usize) {
        let mut editor = self.editor.borrow_mut();
        editor.set_cols(cols);
    }

    pub fn consume_text(&self, text: &str) -> Result<()> {
        let mut editor = self.editor.borrow_mut();
        editor.consume_text(text)?;
        Ok(())
    }

    pub fn consume_key(&self, key: KeyEvent) -> Result<ShellSignal> {
        let mut editor = self.editor.borrow_mut();

        match editor.consume_key(key)? {
            Signal::KeepEditing => Ok(ShellSignal::Continue),
            Signal::InputCompleted(query) => {
                let query = query.to_string();
                let mut session = self.session.borrow_mut();
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
                Ok(ShellSignal::ExecutePending)
            }
            Signal::Exit => Ok(ShellSignal::Exit),
        }
    }

    // TODO: The refcell stuff here is definitely prone to erroring. I'd prefer
    // some sort of message passing approach.
    #[allow(clippy::await_holding_refcell_ref)]
    pub async fn execute_pending(&self) -> Result<()> {
        let mut editor = self.editor.borrow_mut();
        let width = editor.get_cols();

        let mut session = self.session.borrow_mut();
        match session.as_mut() {
            Some(session) => {
                let query = match session.pending.take() {
                    Some(query) => query,
                    None => return Ok(()), // Nothing to execute.
                };
                let mut writer = editor.raw_writer();
                writer.write_all(&[b'\n'])?;

                match session.session.simple(&query).await {
                    Ok(results) => {
                        trace!("writing results");
                        for result in results {
                            match Self::format_execution_stream(result, width).await {
                                Ok(table) => {
                                    writeln!(writer, "{table}")?;
                                }
                                Err(e) => {
                                    // Same as below, the error is related to
                                    // executing a query.
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

                writer.write_all(&[b'\n'])?;
                editor.edit_start()?;

                Ok(())
            }
            None => Err(RayexecError::new(
                "Attempted to run query without attached session",
            )),
        }
    }

    /// Collects the entire stream in memory and creates a pretty table from the
    /// stream.
    async fn format_execution_stream(result: ExecutionResult, width: usize) -> Result<PrettyTable> {
        let batches = result
            .stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        let table = PrettyTable::try_new(&result.output_schema, &batches, width, None)?;

        Ok(table)
    }
}
