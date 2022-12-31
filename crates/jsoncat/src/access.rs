//! Access methods.
use access::external::bigquery::BigQueryTableAccess;
use access::external::postgres::PostgresTableAccess;
use serde::{Deserialize, Serialize};
use std::fmt;

/// How we access tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessMethod {
    Unknown,
    InternalMemory,
    Postgres(PostgresTableAccess),
    BigQuery(BigQueryTableAccess),
}

impl fmt::Display for AccessMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessMethod::Unknown => write!(f, "unknown"),
            AccessMethod::InternalMemory => write!(f, "internal memory"),
            AccessMethod::Postgres(_) => write!(f, "postgres"),
            AccessMethod::BigQuery(_) => write!(f, "bigquery"),
        }
    }
}
