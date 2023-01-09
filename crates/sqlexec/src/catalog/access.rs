//! Access methods.
use datasource_bigquery::BigQueryTableAccess;
use datasource_object_store::gcs::GcsTableAccess;
use datasource_object_store::local::LocalTableAccess;
use datasource_postgres::PostgresTableAccess;
use serde::{Deserialize, Serialize};
use std::fmt;

/// How we access tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessMethod {
    Unknown,
    System,
    Postgres(PostgresTableAccess),
    BigQuery(BigQueryTableAccess),
    Gcs(GcsTableAccess),
    Local(LocalTableAccess),
}

impl fmt::Display for AccessMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessMethod::Unknown => write!(f, "unknown"),
            AccessMethod::System => write!(f, "system"),
            AccessMethod::Postgres(_) => write!(f, "postgres"),
            AccessMethod::BigQuery(_) => write!(f, "bigquery"),
            AccessMethod::Gcs(_) => write!(f, "gcs"),
            AccessMethod::Local(_) => write!(f, "local"),
        }
    }
}
