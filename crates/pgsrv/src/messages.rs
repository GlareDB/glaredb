use datafusion::arrow::record_batch::RecordBatch;
use pgrepr::format::Format;
use sqlexec::errors::ExecError;
use std::collections::HashMap;

use crate::errors::PgSrvError;

/// Version number (v3.0) used during normal frontend startup.
pub const VERSION_V3: i32 = 0x30000;
/// Version number used to request a cancellation.
pub const VERSION_CANCEL: i32 = (1234 << 16) ^ 5678;
/// Version number used to request an SSL connection.
pub const VERSION_SSL: i32 = (1234 << 16) ^ 5679;

/// Messages sent by the frontend during connection startup.
#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
pub enum StartupMessage {
    SSLRequest {
        version: i32,
    },
    CancelRequest {
        version: i32,
    },
    StartupRequest {
        version: i32,
        params: HashMap<String, String>,
    },
}

/// Messages sent by the frontend.
#[derive(Debug)]
pub enum FrontendMessage {
    /// A query (or queries) to execute.
    Query { sql: String },
    /// An encrypted or unencrypted password.
    PasswordMessage { password: String },
    /// An extended query parse message.
    Parse {
        /// The name of the prepared statement. An empty string denotes the
        /// unnamed prepared statement.
        name: String,
        /// The query string to be parsed.
        sql: String,
        /// The object IDs of the parameter data types. Placing a zero here is
        /// equivalent to leaving the type unspecified.
        param_types: Vec<i32>,
    },
    Bind {
        /// The name of the destination portal (an empty string selects the
        /// unnamed portal).
        portal: String,
        /// The name of the source prepared statement (an empty string selects
        /// the unnamed prepared statement).
        statement: String,
        /// The parameter format codes. Each must presently be zero (text) or
        /// one (binary).
        ///
        /// Valid lengths may be:
        /// 0 -> Use default format for all inputs (text)
        /// 1 -> Use this one format for all inputs
        /// n -> Individually specified formats for each input.
        param_formats: Vec<Format>,
        /// The parameter values, in the format indicated by the associated
        /// format code. n is the above length.
        param_values: Vec<Option<Vec<u8>>>,
        /// The result-column format codes. Each must presently be zero (text)
        /// or one (binary).
        ///
        /// Valid lengths may be:
        /// 0 -> Use default format for all outputs (text)
        /// 1 -> Use this one format for all outputs
        /// n -> Individually specified formats for each output.
        result_formats: Vec<Format>,
    },
    Describe {
        /// The kind of item to describe: 'S' to describe a prepared statement;
        /// or 'P' to describe a portal.
        object_type: DescribeObjectType,
        /// The name of the item to describe (an empty string selects the
        /// unnamed prepared statement or portal).
        name: String,
    },
    Execute {
        /// The name of the portal to execute (an empty string selects the
        /// unnamed portal).
        portal: String,
        /// The maximum number of rows to return, if portal contains a query
        /// that returns rows (ignored otherwise). Zero denotes "no limit".
        max_rows: i32,
    },
    Close {
        /// The kind of item to close (portal or statement).
        object_type: DescribeObjectType,
        /// Name of the object to close.
        name: String,
    },
    /// Synchronize after running through the extended query protocol.
    Sync,
    /// Flush the connection.
    Flush,
    /// Close the connection.
    Terminate,
}

impl FrontendMessage {
    pub const fn name(&self) -> &'static str {
        match self {
            FrontendMessage::Query { .. } => "query",
            FrontendMessage::PasswordMessage { .. } => "password",
            FrontendMessage::Parse { .. } => "parse",
            FrontendMessage::Bind { .. } => "bind",
            FrontendMessage::Describe { .. } => "describe",
            FrontendMessage::Execute { .. } => "execute",
            FrontendMessage::Close { .. } => "close",
            FrontendMessage::Flush => "flush",
            FrontendMessage::Sync => "sync",
            FrontendMessage::Terminate => "terminate",
        }
    }
}

#[derive(Debug)]
pub enum TransactionStatus {
    Idle,
    InBlock,
    Failed,
}

#[derive(Debug)]
pub enum BackendMessage {
    ErrorResponse(ErrorResponse),
    NoticeResponse(NoticeResponse),
    AuthenticationOk,
    AuthenticationCleartextPassword,
    BackendKeyData { pid: i32, secret: i32 },
    ParameterStatus { key: String, val: String },
    EmptyQueryResponse,
    ReadyForQuery(TransactionStatus),
    CommandComplete { tag: String },
    RowDescription(Vec<FieldDescription>),
    DataRow(RecordBatch, usize),
    ParseComplete,
    BindComplete,
    CloseComplete,
    NoData,
    ParameterDescription(Vec<i32>),
}

impl From<ErrorResponse> for BackendMessage {
    fn from(error: ErrorResponse) -> Self {
        BackendMessage::ErrorResponse(error)
    }
}

impl From<NoticeResponse> for BackendMessage {
    fn from(notice: NoticeResponse) -> Self {
        BackendMessage::NoticeResponse(notice)
    }
}

#[derive(Debug)]
pub enum ErrorSeverity {
    Error,
    Fatal,
    Panic,
}

impl ErrorSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorSeverity::Error => "ERROR",
            ErrorSeverity::Fatal => "FATAL",
            ErrorSeverity::Panic => "PANIC",
        }
    }
}

/// 'SQLSTATE' error codes.
///
/// See a complete list here: https://www.postgresql.org/docs/current/errcodes-appendix.html
#[derive(Debug)]
pub enum SqlState {
    // Class 00 — Successful Completion
    Successful,

    // Class 01 — Warning
    Warning,

    // Class 0A — Feature Not Supported
    FeatureNotSupported,

    // Class 42 — Syntax Error or Access Rule Violation
    SyntaxError,

    // Class XX — Internal Error
    InternalError,
}

impl SqlState {
    pub fn as_code_str(&self) -> &'static str {
        match self {
            SqlState::Successful => "00000",
            SqlState::Warning => "01000",
            SqlState::FeatureNotSupported => "0A000",
            SqlState::SyntaxError => "42601",
            SqlState::InternalError => "XX000",
        }
    }
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub severity: ErrorSeverity,
    pub code: SqlState,
    pub message: String,
}

impl ErrorResponse {
    pub fn error(code: SqlState, msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Error,
            code,
            message: msg.into(),
        }
    }

    pub fn feature_not_supported(msg: impl Into<String>) -> ErrorResponse {
        Self::error(SqlState::FeatureNotSupported, msg)
    }

    pub fn error_internal(msg: impl Into<String>) -> ErrorResponse {
        Self::error(SqlState::InternalError, msg)
    }

    pub fn fatal_internal(msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Fatal,
            code: SqlState::InternalError,
            message: msg.into(),
        }
    }
}

impl From<ExecError> for ErrorResponse {
    fn from(e: ExecError) -> Self {
        // TODO: Actually set appropriate codes.
        ErrorResponse::error_internal(e.to_string())
    }
}

#[derive(Debug)]
pub enum NoticeSeverity {
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl NoticeSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            NoticeSeverity::Warning => "WARNING",
            NoticeSeverity::Notice => "NOTICE",
            NoticeSeverity::Debug => "DEBUG",
            NoticeSeverity::Info => "INFO",
            NoticeSeverity::Log => "LOG",
        }
    }
}

#[derive(Debug)]
pub struct NoticeResponse {
    pub severity: NoticeSeverity,
    pub code: SqlState,
    pub message: String,
}

impl NoticeResponse {
    pub fn info(msg: impl Into<String>) -> NoticeResponse {
        NoticeResponse {
            severity: NoticeSeverity::Info,
            code: SqlState::Successful,
            message: msg.into(),
        }
    }
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: String,
    pub table_id: i32,
    pub col_id: i16,
    pub obj_id: i32,
    pub type_size: i16,
    pub type_mod: i32,
    pub format: i16,
}

impl FieldDescription {
    pub fn new_named(name: impl Into<String>) -> FieldDescription {
        FieldDescription {
            name: name.into(),
            table_id: 0,
            col_id: 0,
            obj_id: 0,
            type_size: 0,
            type_mod: 0,
            format: 0, // Text
        }
    }
}

#[derive(Debug)]
#[repr(u8)]
pub enum DescribeObjectType {
    Statement = b'S',
    Portal = b'P',
}

impl std::fmt::Display for DescribeObjectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DescribeObjectType::Statement => write!(f, "Statement"),
            DescribeObjectType::Portal => write!(f, "Portal"),
        }
    }
}

impl TryFrom<u8> for DescribeObjectType {
    type Error = PgSrvError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            b'S' => Ok(DescribeObjectType::Statement),
            b'P' => Ok(DescribeObjectType::Portal),
            _ => Err(PgSrvError::UnexpectedDescribeObjectType(value)),
        }
    }
}
