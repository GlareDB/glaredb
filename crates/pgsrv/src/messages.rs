use datafusion::arrow::record_batch::RecordBatch;
use std::collections::HashMap;

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
    EmptyQueryResponse,
    ReadyForQuery(TransactionStatus),
    CommandComplete { tag: String },
    RowDescription(Vec<FieldDescription>),
    DataRow(RecordBatch, usize),
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
    Successful,
    Warning,
    FeatureNotSupported,
    InternalError,
}

impl SqlState {
    pub fn as_code_str(&self) -> &'static str {
        match self {
            SqlState::Successful => "00000",
            SqlState::Warning => "01000",
            SqlState::FeatureNotSupported => "0A000",
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
    pub fn feature_not_supported(msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Error,
            code: SqlState::FeatureNotSupported,
            message: msg.into(),
        }
    }

    pub fn error_internal(msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Error,
            code: SqlState::InternalError,
            message: msg.into(),
        }
    }

    pub fn fatal_internal(msg: impl Into<String>) -> ErrorResponse {
        ErrorResponse {
            severity: ErrorSeverity::Fatal,
            code: SqlState::InternalError,
            message: msg.into(),
        }
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
