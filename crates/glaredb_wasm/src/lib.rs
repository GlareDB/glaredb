pub mod errors;
pub mod runtime;
pub mod session;
pub mod shell;

mod http;
mod time;
mod tracing;

use ::tracing::trace;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub fn init_panic_handler() {
    trace!("init panic handler");
    console_error_panic_hook::set_once();
}
