use crate::{errors::Result, runtime::WasmExecutionRuntime};
use js_sys::Function;
use rayexec_execution::datasource::{DataSourceRegistry, MemoryDataSource};
use rayexec_execution::engine::Engine;
use rayexec_parquet::ParquetDataSource;
use rayexec_shell::shell::ShellSignal;
use rayexec_shell::{lineedit::KeyEvent, shell::Shell};
use std::io::{self, BufWriter};
use std::rc::Rc;
use std::sync::Arc;
use tracing::{error, trace, warn};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::KeyboardEvent;

#[wasm_bindgen]
extern "C" {
    /// Structurally typed terminal interface for outputting data including
    /// terminal codes.
    ///
    /// xterm.js mostly.
    #[derive(Debug)]
    pub type Terminal;

    #[wasm_bindgen(method)]
    pub fn write(this: &Terminal, data: &[u8]);

    #[wasm_bindgen(js_name = "IDisposable")]
    pub type Disposable;

    #[wasm_bindgen(method, js_name = "dispose")]
    pub fn dispose(this: &Disposable);

    pub type OnKeyEvent;

    #[wasm_bindgen(method, getter, js_name = "key")]
    pub fn key(this: &OnKeyEvent) -> String;

    #[wasm_bindgen(method, getter, js_name = "domEvent")]
    pub fn dom_event(this: &OnKeyEvent) -> KeyboardEvent;

    #[wasm_bindgen(method, js_name = "onKey")]
    pub fn on_key(
        this: &Terminal,
        f: &Function, // Event<{key: &str, dom_event: KeyboardEvent}>
    ) -> Disposable;
}

#[derive(Debug)]
pub struct TerminalWrapper {
    terminal: Terminal,
}

impl TerminalWrapper {
    pub fn new(terminal: Terminal) -> Self {
        TerminalWrapper { terminal }
    }
}

impl io::Write for TerminalWrapper {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = buf.len();
        self.terminal.write(buf);

        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Wrapper around a database session and the engine that created it.
///
/// This is expected to be instantiated on the javascript side.
#[wasm_bindgen]
#[derive(Debug)]
#[allow(dead_code)]
pub struct WasmShell {
    pub(crate) engine: Engine,
    // TODO: For some reason, without the buf writer, the output gets all messed
    // up. The buf writer is a good thing since we're calling flush where
    // appropriate, but it'd be nice to know what's going wrong when it's not
    // used.
    pub(crate) shell: Rc<Shell<BufWriter<TerminalWrapper>>>,
}

#[wasm_bindgen]
impl WasmShell {
    /// Create a new shell that writes its output to the provided terminal.
    pub fn try_new(terminal: Terminal) -> Result<WasmShell> {
        let runtime = Arc::new(WasmExecutionRuntime::try_new()?);
        let registry = DataSourceRegistry::default()
            .with_datasource("memory", Box::new(MemoryDataSource))?
            .with_datasource("parquet", Box::new(ParquetDataSource))?;

        let engine = Engine::new_with_registry(runtime, registry)?;

        let terminal = TerminalWrapper::new(terminal);
        let shell = Rc::new(Shell::new(BufWriter::new(terminal)));

        let session = engine.new_session()?;
        shell.attach(session, "Rayexec WASM Shell")?;

        Ok(WasmShell { engine, shell })
    }

    /// Notify on terminal resize.
    pub fn on_resize(&self, cols: usize) {
        trace!(%cols, "setting columns");
        self.shell.set_cols(cols)
    }

    /// Consume a string verbatim.
    ///
    /// This should be hooked up to xterm's `onData` method to enable pasting.
    pub fn on_data(&self, text: String) -> Result<()> {
        self.shell.consume_text(&text)?;
        Ok(())
    }

    /// Consume a keyboard event.
    ///
    /// This should be hooked up to xterm's `attachCustomKeyEventHandler`. This
    /// will handle _all_ input, so `false` should be returned to xterm after
    /// calling this for an event.
    pub fn on_key(&self, event: KeyboardEvent) -> Result<()> {
        if event.type_() != "keydown" {
            return Ok(());
        }

        let key = event.key();

        let key = match key.as_str() {
            "Backspace" => KeyEvent::Backspace,
            "Enter" => KeyEvent::Enter,
            other if other.chars().count() != 1 => {
                warn!(%other, "unhandled input");
                return Ok(());
            }
            other => match other.chars().next() {
                Some(ch) => {
                    if event.ctrl_key() {
                        match ch {
                            'c' => KeyEvent::CtrlC,
                            other => {
                                warn!(%other, "unhandled input with ctrl modifier");
                                return Ok(());
                            }
                        }
                    } else if event.meta_key() {
                        warn!(%other, "unhandled input with meta modifier");
                        return Ok(());
                    } else {
                        KeyEvent::Char(ch)
                    }
                }
                None => {
                    warn!("key event with no key");
                    return Ok(());
                }
            },
        };

        match self.shell.consume_key(key)? {
            ShellSignal::Continue => (), // Continue with normal editing.
            ShellSignal::ExecutePending => {
                let shell = self.shell.clone();
                spawn_local(async move {
                    if let Err(e) = shell.execute_pending().await {
                        error!(%e, "error executing pending query");
                    }
                });
            }
            ShellSignal::Exit => (), // Can't exit out of the web shell.
        }

        Ok(())
    }
}
