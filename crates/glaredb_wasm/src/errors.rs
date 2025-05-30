use std::error::Error;
use std::fmt;

use glaredb_error::DbError;
use wasm_bindgen::JsValue;

pub type Result<T, E = WasmError> = std::result::Result<T, E>;

/// A wrapper around a rayexec error which can be convert the error to a JsValue
/// (a string).
#[derive(Debug)]
pub struct WasmError {
    pub error: DbError,
}

impl Error for WasmError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.error.source()
    }
}

impl fmt::Display for WasmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl From<DbError> for WasmError {
    fn from(value: DbError) -> Self {
        WasmError { error: value }
    }
}

impl From<WasmError> for JsValue {
    fn from(value: WasmError) -> Self {
        JsValue::from_str(&value.to_string())
    }
}

/// Helper for JSON stringifying a javascript value.
#[allow(unused)]
pub(crate) fn json_stringify(value: &JsValue) -> String {
    // this is amazing
    js_sys::JSON::stringify(value)
        .map(|js_str| format!("{js_str}"))
        .unwrap_or_default()
}
