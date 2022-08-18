use crate::errors::Result;
use std::collections::HashMap;

/// Protocol version number (v3.0).
pub const VERSION: i32 = 0x30000;

#[derive(Debug)]
pub enum StartupMessage {
    Startup {
        version: i32,
        params: HashMap<String, String>,
    },
}

/// Messages sent from the frontend.
#[derive(Debug)]
pub enum FrontendMessage {
    /// A query (or queries) to execute.
    Query { sql: String },
    /// An encrypted or unencrypted password.
    PasswordMessage { password: String },
}

#[derive(Debug)]
pub enum BackendMessage {
    ErrorResponse(BackendError),
    AuthenticationOk,
    AuthenticationCleartextPassword,
    ReadyForQuery,
    CommandComplete,
    RowDescription,
}

#[derive(Debug)]
pub struct BackendError {}
