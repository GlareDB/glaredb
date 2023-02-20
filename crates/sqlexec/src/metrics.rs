use crate::errors::Result;
use serde_json::json;
use uuid::Uuid;

#[derive(Debug)]
pub enum ExecutionStatus {
    Success,
    Fail,
    Unknown,
}

impl ExecutionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ExecutionStatus::Success => "success",
            ExecutionStatus::Fail => "fail",
            ExecutionStatus::Unknown => "unknown",
        }
    }
}

/// A set of metrics for a single query.
///
/// Every query should have a set of metrics associated with it. Some fields may
/// not be relevant for a particular query type, and should be Null/None in such
/// cases.
#[derive(Debug)]
pub struct QueryMetrics {
    pub database_id: Uuid,

    pub user_id: Uuid,

    pub conn_id: Uuid,

    /// The text representation of the query.
    pub query_text: String,

    pub execution_status: ExecutionStatus,
    pub error_message: Option<String>,

    /// Elapsed compute time in ns.
    pub elapsed_compute: u64,

    /// Number of output rows in the query.
    pub output_rows: Option<u32>,
}

impl QueryMetrics {
    pub fn to_json_value(&self) -> serde_json::Value {
        json!({
            "database_id": self.database_id.to_string(),
            "user_id": self.user_id.to_string(),
            "conn_id": self.conn_id.to_string(),
            "query_text": self.query_text,
            "execution_status": self.execution_status.as_str(),
            "error_message": self.error_message.clone().unwrap_or_default(),
            "elapsed_compute": self.elapsed_compute,
            "output_rows": self.output_rows.unwrap_or(0),
        })
    }
}
