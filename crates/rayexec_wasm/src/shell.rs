use std::io::{self, BufWriter};
use std::rc::Rc;

use js_sys::Function;
use rayexec_execution::engine::single_user::SingleUserEngine;
use rayexec_execution::shell::lineedit::{KeyEvent, TermSize};
use rayexec_execution::shell::shell::{RawModeGuard, RawModeTerm, Shell, ShellSignal};
use tracing::{error, trace, warn};
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::KeyboardEvent;

use crate::errors::Result;
use crate::runtime::{WasmExecutor, WasmRuntime};

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
    // TODO: For some reason, without the buf writer, the output gets all messed
    // up. The buf writer is a good thing since we're calling flush where
    // appropriate, but it'd be nice to know what's going wrong when it's not
    // used.
    pub(crate) shell: Rc<Shell<BufWriter<TerminalWrapper>, WasmExecutor, WasmRuntime, NopRawMode>>,
    /// Updateable guard used for shell inputs.
    edit_guard: Option<RawModeGuard<NopRawMode>>,
}

/// Implementation of raw mode term that does nothing.
///
/// xterm.js is essentially always in raw mode, there no notion of "cooked" mode
/// for it, so there's no state for use to return to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NopRawMode;

impl RawModeTerm for NopRawMode {
    fn enable_raw_mode(&self) {}
    fn disable_raw_mode(&self) {}
}

#[wasm_bindgen]
impl WasmShell {
    /// Create a new shell that writes its output to the provided terminal.
    pub fn try_new(terminal: Terminal) -> Result<WasmShell> {
        let runtime = WasmRuntime::try_new()?;
        let engine = SingleUserEngine::try_new(WasmExecutor, runtime)?;

        let terminal = TerminalWrapper::new(terminal);
        let shell = Rc::new(Shell::new(BufWriter::new(terminal), NopRawMode));

        shell.attach(engine, "GlareDB WASM Shell")?;
        let edit_guard = shell.edit_start()?;

        Ok(WasmShell {
            shell,
            edit_guard: Some(edit_guard),
        })
    }

    /// Notify on terminal resize.
    pub fn on_resize(&self, cols: usize) {
        trace!(%cols, "setting columns");
        self.shell.set_size(TermSize { cols });
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
    pub fn on_key(&mut self, event: KeyboardEvent) -> Result<()> {
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

        let guard = match self.edit_guard.take() {
            Some(guard) => guard,
            None => self.shell.edit_start()?, // TODO: Shouldn't happen.
        };

        match self.shell.consume_key(guard, key)? {
            ShellSignal::Continue(guard) => {
                self.edit_guard = Some(guard);
                // Continue with normal editing.
            }
            ShellSignal::ExecutePending(guard) => {
                let shell = self.shell.clone();
                spawn_local(async move {
                    if let Err(e) = shell.execute_pending(guard).await {
                        error!(%e, "error executing pending query");
                    }
                });
                self.edit_guard = Some(self.shell.edit_start()?);
            }
            ShellSignal::Exit => (), // Can't exit out of the web shell.
        }

        Ok(())
    }
}
