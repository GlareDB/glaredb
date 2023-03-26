use super::{FromOptionalField, ProtoConvError};
use crate::proto::arrow;
use crate::proto::catalog;
use crate::proto::options;
use datafusion::arrow::datatypes::DataType;
use proptest_derive::Arbitrary;
use std::collections::HashMap;
use std::fmt;

// Database options

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum DatabaseOptions {
    Internal(DatabaseOptionsInternal),
    Debug(DatabaseOptionsDebug),
    Postgres(DatabaseOptionsPostgres),
    BigQuery(DatabaseOptionsBigQuery),
    Mysql(DatabaseOptionsMysql),
    Mongo(DatabaseOptionsMongo),
}

impl DatabaseOptions {
    pub const INTERNAL: &str = "internal";
    pub const DEBUG: &str = "debug";
    pub const POSTGRES: &str = "postgres";
    pub const BIGQUERY: &str = "bigquery";
    pub const MYSQL: &str = "mysql";
    pub const MONGO: &str = "mongo";

    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseOptions::Internal(_) => Self::INTERNAL,
            DatabaseOptions::Debug(_) => Self::DEBUG,
            DatabaseOptions::Postgres(_) => Self::POSTGRES,
            DatabaseOptions::BigQuery(_) => Self::BIGQUERY,
            DatabaseOptions::Mysql(_) => Self::MYSQL,
            DatabaseOptions::Mongo(_) => Self::MONGO,
        }
    }
}

impl fmt::Display for DatabaseOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<options::database_options::Options> for DatabaseOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::database_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            options::database_options::Options::Internal(v) => {
                DatabaseOptions::Internal(v.try_into()?)
            }
            options::database_options::Options::Debug(v) => DatabaseOptions::Debug(v.try_into()?),
            options::database_options::Options::Postgres(v) => {
                DatabaseOptions::Postgres(v.try_into()?)
            }
            options::database_options::Options::Bigquery(v) => {
                DatabaseOptions::BigQuery(v.try_into()?)
            }
            options::database_options::Options::Mysql(v) => DatabaseOptions::Mysql(v.try_into()?),
            options::database_options::Options::Mongo(v) => DatabaseOptions::Mongo(v.try_into()?),
        })
    }
}

impl TryFrom<options::DatabaseOptions> for DatabaseOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<DatabaseOptions> for options::database_options::Options {
    fn from(value: DatabaseOptions) -> Self {
        match value {
            DatabaseOptions::Internal(v) => options::database_options::Options::Internal(v.into()),
            DatabaseOptions::Debug(v) => options::database_options::Options::Debug(v.into()),
            DatabaseOptions::Postgres(v) => options::database_options::Options::Postgres(v.into()),
            DatabaseOptions::BigQuery(v) => options::database_options::Options::Bigquery(v.into()),
            DatabaseOptions::Mysql(v) => options::database_options::Options::Mysql(v.into()),
            DatabaseOptions::Mongo(v) => options::database_options::Options::Mongo(v.into()),
        }
    }
}

impl From<DatabaseOptions> for options::DatabaseOptions {
    fn from(value: DatabaseOptions) -> Self {
        options::DatabaseOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseOptionsInternal {}

impl TryFrom<options::DatabaseOptionsInternal> for DatabaseOptionsInternal {
    type Error = ProtoConvError;
    fn try_from(_value: options::DatabaseOptionsInternal) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsInternal {})
    }
}

impl From<DatabaseOptionsInternal> for options::DatabaseOptionsInternal {
    fn from(_value: DatabaseOptionsInternal) -> Self {
        options::DatabaseOptionsInternal {}
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseOptionsDebug {}

impl TryFrom<options::DatabaseOptionsDebug> for DatabaseOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(_value: options::DatabaseOptionsDebug) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsDebug {})
    }
}

impl From<DatabaseOptionsDebug> for options::DatabaseOptionsDebug {
    fn from(_value: DatabaseOptionsDebug) -> Self {
        options::DatabaseOptionsDebug {}
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseOptionsPostgres {
    pub connection_string: String,
}

impl TryFrom<options::DatabaseOptionsPostgres> for DatabaseOptionsPostgres {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsPostgres) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsPostgres {
            connection_string: value.connection_string,
        })
    }
}

impl From<DatabaseOptionsPostgres> for options::DatabaseOptionsPostgres {
    fn from(value: DatabaseOptionsPostgres) -> Self {
        options::DatabaseOptionsPostgres {
            connection_string: value.connection_string,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseOptionsBigQuery {
    pub service_account_key: String,
    pub project_id: String,
}

impl TryFrom<options::DatabaseOptionsBigQuery> for DatabaseOptionsBigQuery {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsBigQuery) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsBigQuery {
            service_account_key: value.service_account_key,
            project_id: value.project_id,
        })
    }
}

impl From<DatabaseOptionsBigQuery> for options::DatabaseOptionsBigQuery {
    fn from(value: DatabaseOptionsBigQuery) -> Self {
        options::DatabaseOptionsBigQuery {
            service_account_key: value.service_account_key,
            project_id: value.project_id,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseOptionsMysql {
    pub connection_string: String,
}

impl TryFrom<options::DatabaseOptionsMysql> for DatabaseOptionsMysql {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsMysql) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsMysql {
            connection_string: value.connection_string,
        })
    }
}

impl From<DatabaseOptionsMysql> for options::DatabaseOptionsMysql {
    fn from(value: DatabaseOptionsMysql) -> Self {
        options::DatabaseOptionsMysql {
            connection_string: value.connection_string,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct DatabaseOptionsMongo {
    pub connection_string: String,
}

impl TryFrom<options::DatabaseOptionsMongo> for DatabaseOptionsMongo {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsMongo) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsMongo {
            connection_string: value.connection_string,
        })
    }
}

impl From<DatabaseOptionsMongo> for options::DatabaseOptionsMongo {
    fn from(value: DatabaseOptionsMongo) -> Self {
        options::DatabaseOptionsMongo {
            connection_string: value.connection_string,
        }
    }
}

// Table options
