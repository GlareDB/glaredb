use std::cell::RefCell;
use std::io::{self, Write};

use rayexec_error::{RayexecError, Result};
use tracing::trace;

use super::lineedit::{KeyEvent, LineEditor, Signal};
use super::vt100::{MODES_OFF, MODE_BOLD};
use crate::arrays::format::pretty::table::PrettyTable;
use crate::engine::single_user::SingleUserEngine;
use crate::runtime::{PipelineExecutor, Runtime};

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
pub struct Shell<W: io::Write, P: PipelineExecutor, R: Runtime> {
    editor: RefCell<LineEditor<W>>,
    engine: RefCell<Option<EngineWithConfig<P, R>>>,
}

#[derive(Debug)]
struct EngineWithConfig<P: PipelineExecutor, R: Runtime> {
    engine: SingleUserEngine<P, R>,
    pending: Option<String>,
}

impl<W, P, R> Shell<W, P, R>
where
    W: io::Write,
    P: PipelineExecutor,
    R: Runtime,
{
    pub fn new(writer: W) -> Self {
        let editor = LineEditor::new(writer, ">> ", 80);
        Shell {
            editor: RefCell::new(editor),
            engine: RefCell::new(None),
        }
    }

    pub fn attach(&self, engine: SingleUserEngine<P, R>, shell_msg: &str) -> Result<()> {
        let mut current = self.engine.borrow_mut();
        *current = Some(EngineWithConfig {
            engine,
            pending: None,
        });

        let mut editor = self.editor.borrow_mut();
        writeln!(editor.raw_writer(), "{}{shell_msg}{}", MODE_BOLD, MODES_OFF)?;
        let version = env!("CARGO_PKG_VERSION");
        writeln!(
            editor.raw_writer(),
            "Preview ({version}) - There will be bugs!"
        )?;
        editor.raw_writer().write_all(b"\n")?;

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

        let mut engine = self.engine.borrow_mut();
        match engine.as_mut() {
            Some(engine) => {
                let query = match engine.pending.take() {
                    Some(query) => query,
                    None => return Ok(()), // Nothing to execute.
                };
                let mut writer = editor.raw_writer();
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

                writer.write_all(b"\n")?;
                editor.edit_start()?;

                Ok(())
            }
            None => Err(RayexecError::new(
                "Attempted to run query without attached engine",
            )),
        }
    }
}
