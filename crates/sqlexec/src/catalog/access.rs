//! Access methods.
use std::fmt;

use datasource_bigquery::BigQueryTableAccess;
use datasource_debug::DebugTableType;
use datasource_object_store::gcs::GcsTableAccess;
use datasource_object_store::local::LocalTableAccess;
use datasource_object_store::s3::S3TableAccess;
use datasource_postgres::PostgresTableAccess;
use serde::{Deserialize, Serialize};

/// How we access tables.
// TODO: We might want to have this enum determine whether or not we persist
// tables. We likely don't want to ever persist 'system' or 'debug' tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AccessMethod {
    Unknown,
    System,
    Postgres(PostgresTableAccess),
    BigQuery(BigQueryTableAccess),
    Gcs(GcsTableAccess),
    S3(S3TableAccess),
    Local(LocalTableAccess),
    Debug(DebugTableType),
}

impl fmt::Display for AccessMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AccessMethod::Unknown => write!(f, "unknown"),
            AccessMethod::System => write!(f, "system"),
            AccessMethod::Postgres(_) => write!(f, "postgres"),
            AccessMethod::BigQuery(_) => write!(f, "bigquery"),
            AccessMethod::Gcs(_) => write!(f, "gcs"),
            AccessMethod::S3(_) => write!(f, "S3"),
            AccessMethod::Local(_) => write!(f, "local"),
            AccessMethod::Debug(_) => write!(f, "debug"),
        }
    }
}
