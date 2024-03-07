use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use std::hash::Hash;

use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef};
use datafusion::common::DFSchemaRef;

use crate::gen::common::arrow;
use crate::gen::metastore::options;
use crate::{FromOptionalField, ProtoConvError};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InternalColumnDefinition {
    pub name: String,
    pub nullable: bool,
    pub arrow_type: DataType,
}

impl From<InternalColumnDefinition> for OptionValue {
    fn from(value: InternalColumnDefinition) -> Self {
        OptionValue::Object(
            vec![
                ("name".to_string(), OptionValue::String(value.name)),
                ("nullable".to_string(), OptionValue::Bool(value.nullable)),
                (
                    "arrow_type".to_string(),
                    OptionValue::String(value.arrow_type.to_string()),
                ),
            ]
            .into_iter()
            .collect(),
        )
    }
}

impl TryFrom<OptionValue> for InternalColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        if let OptionValue::Object(mut obj) = value {
            let name = obj
                .remove("name")
                .ok_or_else(|| ProtoConvError::RequiredField("name".to_string()))?;
            let nullable = obj
                .remove("nullable")
                .ok_or_else(|| ProtoConvError::RequiredField("nullable".to_string()))?;
            let arrow_type = obj
                .remove("arrow_type")
                .ok_or_else(|| ProtoConvError::RequiredField("arrow_type".to_string()))?;
            let arrow_type: DataType = arrow_type.try_into()?;

            Ok(InternalColumnDefinition {
                name: name.try_into()?,
                nullable: nullable.try_into()?,
                arrow_type: arrow_type.try_into()?,
            })
        } else {
            Err(ProtoConvError::ParseError("Expected object".to_string()))
        }
    }
}

impl InternalColumnDefinition {
    /// Create a vec of column definitions.
    ///
    /// Tuples are in the form of:
    /// (name, datatype, nullable)
    pub fn from_tuples<C, N>(cols: C) -> Vec<InternalColumnDefinition>
    where
        C: IntoIterator<Item = (N, DataType, bool)>,
        N: Into<String>,
    {
        cols.into_iter()
            .map(|(name, arrow_type, nullable)| InternalColumnDefinition {
                name: name.into(),
                nullable,
                arrow_type,
            })
            .collect()
    }

    /// Create a vec of column definitions from arrow fields.
    pub fn from_arrow_fields<C>(cols: C) -> Vec<InternalColumnDefinition>
    where
        C: IntoIterator<Item = Field>,
    {
        cols.into_iter()
            .map(|field| InternalColumnDefinition {
                name: field.name().clone(),
                nullable: field.is_nullable(),
                arrow_type: field.data_type().clone(),
            })
            .collect()
    }
}

impl TryFrom<options::InternalColumnDefinition> for InternalColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: options::InternalColumnDefinition) -> Result<Self, Self::Error> {
        let arrow_type: DataType = value.arrow_type.as_ref().required("arrow_type")?;
        Ok(InternalColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type,
        })
    }
}

// TODO: Try to make this just `From`. Would require some additional conversions
// for the arrow types.
impl TryFrom<InternalColumnDefinition> for options::InternalColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: InternalColumnDefinition) -> Result<Self, Self::Error> {
        let arrow_type = arrow::ArrowType::try_from(&value.arrow_type)?;
        Ok(options::InternalColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type: Some(arrow_type),
        })
    }
}

// Database options

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DatabaseOptions {
    Internal(DatabaseOptionsInternal),
    Debug(DatabaseOptionsDebug),
    Postgres(DatabaseOptionsPostgres),
    BigQuery(DatabaseOptionsBigQuery),
    Mysql(DatabaseOptionsMysql),
    MongoDb(DatabaseOptionsMongoDb),
    Snowflake(DatabaseOptionsSnowflake),
    Delta(DatabaseOptionsDeltaLake),
    SqlServer(DatabaseOptionsSqlServer),
    Clickhouse(DatabaseOptionsClickhouse),
    Cassandra(DatabaseOptionsCassandra),
    Sqlite(DatabaseOptionsSqlite),
}

impl DatabaseOptions {
    pub const INTERNAL: &'static str = "internal";
    pub const DEBUG: &'static str = "debug";
    pub const POSTGRES: &'static str = "postgres";
    pub const BIGQUERY: &'static str = "bigquery";
    pub const MYSQL: &'static str = "mysql";
    pub const MONGODB: &'static str = "mongo";
    pub const SNOWFLAKE: &'static str = "snowflake";
    pub const DELTA: &'static str = "delta";
    pub const SQL_SERVER: &'static str = "sql_server";
    pub const CLICKHOUSE: &'static str = "clickhouse";
    pub const CASSANDRA: &'static str = "cassandra";
    pub const SQLITE: &'static str = "sqlite";

    pub fn as_str(&self) -> &'static str {
        match self {
            DatabaseOptions::Internal(_) => Self::INTERNAL,
            DatabaseOptions::Debug(_) => Self::DEBUG,
            DatabaseOptions::Postgres(_) => Self::POSTGRES,
            DatabaseOptions::BigQuery(_) => Self::BIGQUERY,
            DatabaseOptions::Mysql(_) => Self::MYSQL,
            DatabaseOptions::MongoDb(_) => Self::MONGODB,
            DatabaseOptions::Snowflake(_) => Self::SNOWFLAKE,
            DatabaseOptions::Delta(_) => Self::DELTA,
            DatabaseOptions::SqlServer(_) => Self::SQL_SERVER,
            DatabaseOptions::Clickhouse(_) => Self::CLICKHOUSE,
            DatabaseOptions::Cassandra(_) => Self::CASSANDRA,
            DatabaseOptions::Sqlite(_) => Self::SQLITE,
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
            options::database_options::Options::Mongodb(v) => {
                DatabaseOptions::MongoDb(v.try_into()?)
            }
            options::database_options::Options::Snowflake(v) => {
                DatabaseOptions::Snowflake(v.try_into()?)
            }
            options::database_options::Options::Delta(v) => DatabaseOptions::Delta(v.try_into()?),
            options::database_options::Options::SqlServer(v) => {
                DatabaseOptions::SqlServer(v.try_into()?)
            }
            options::database_options::Options::Clickhouse(v) => {
                DatabaseOptions::Clickhouse(v.try_into()?)
            }
            options::database_options::Options::Cassandra(v) => {
                DatabaseOptions::Cassandra(v.try_into()?)
            }
            options::database_options::Options::Sqlite(v) => DatabaseOptions::Sqlite(v.try_into()?),
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
            DatabaseOptions::MongoDb(v) => options::database_options::Options::Mongodb(v.into()),
            DatabaseOptions::Snowflake(v) => {
                options::database_options::Options::Snowflake(v.into())
            }
            DatabaseOptions::Delta(v) => options::database_options::Options::Delta(v.into()),
            DatabaseOptions::SqlServer(v) => {
                options::database_options::Options::SqlServer(v.into())
            }
            DatabaseOptions::Clickhouse(v) => {
                options::database_options::Options::Clickhouse(v.into())
            }
            DatabaseOptions::Cassandra(v) => {
                options::database_options::Options::Cassandra(v.into())
            }
            DatabaseOptions::Sqlite(v) => options::database_options::Options::Sqlite(v.into()),
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsMongoDb {
    pub connection_string: String,
}

impl TryFrom<options::DatabaseOptionsMongoDb> for DatabaseOptionsMongoDb {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsMongoDb) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsMongoDb {
            connection_string: value.connection_string,
        })
    }
}

impl From<DatabaseOptionsMongoDb> for options::DatabaseOptionsMongoDb {
    fn from(value: DatabaseOptionsMongoDb) -> Self {
        options::DatabaseOptionsMongoDb {
            connection_string: value.connection_string,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsSqlServer {
    pub connection_string: String,
}

impl TryFrom<options::DatabaseOptionsSqlServer> for DatabaseOptionsSqlServer {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsSqlServer) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsSqlServer {
            connection_string: value.connection_string,
        })
    }
}

impl From<DatabaseOptionsSqlServer> for options::DatabaseOptionsSqlServer {
    fn from(value: DatabaseOptionsSqlServer) -> Self {
        options::DatabaseOptionsSqlServer {
            connection_string: value.connection_string,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsClickhouse {
    pub connection_string: String,
}

impl TryFrom<options::DatabaseOptionsClickhouse> for DatabaseOptionsClickhouse {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsClickhouse) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsClickhouse {
            connection_string: value.connection_string,
        })
    }
}

impl From<DatabaseOptionsClickhouse> for options::DatabaseOptionsClickhouse {
    fn from(value: DatabaseOptionsClickhouse) -> Self {
        options::DatabaseOptionsClickhouse {
            connection_string: value.connection_string,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsCassandra {
    pub host: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl TryFrom<options::DatabaseOptionsCassandra> for DatabaseOptionsCassandra {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsCassandra) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsCassandra {
            host: value.host,
            username: value.username,
            password: value.password,
        })
    }
}

impl From<DatabaseOptionsCassandra> for options::DatabaseOptionsCassandra {
    fn from(value: DatabaseOptionsCassandra) -> Self {
        options::DatabaseOptionsCassandra {
            host: value.host,
            username: value.username,
            password: value.password,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsSqlite {
    pub location: String,
}

impl TryFrom<options::DatabaseOptionsSqlite> for DatabaseOptionsSqlite {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsSqlite) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsSqlite {
            location: value.location,
        })
    }
}

impl From<DatabaseOptionsSqlite> for options::DatabaseOptionsSqlite {
    fn from(value: DatabaseOptionsSqlite) -> Self {
        options::DatabaseOptionsSqlite {
            location: value.location,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsSnowflake {
    pub account_name: String,
    pub login_name: String,
    pub password: String,
    pub database_name: String,
    pub warehouse: String,
    pub role_name: String,
}

impl TryFrom<options::DatabaseOptionsSnowflake> for DatabaseOptionsSnowflake {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsSnowflake) -> Result<Self, Self::Error> {
        Ok(DatabaseOptionsSnowflake {
            account_name: value.account_name,
            login_name: value.login_name,
            password: value.password,
            database_name: value.database_name,
            warehouse: value.warehouse,
            role_name: value.role_name,
        })
    }
}

impl From<DatabaseOptionsSnowflake> for options::DatabaseOptionsSnowflake {
    fn from(value: DatabaseOptionsSnowflake) -> Self {
        options::DatabaseOptionsSnowflake {
            account_name: value.account_name,
            login_name: value.login_name,
            password: value.password,
            database_name: value.database_name,
            warehouse: value.warehouse,
            role_name: value.role_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatabaseOptionsDeltaLake {
    pub catalog: DeltaLakeCatalog,
    pub storage_options: StorageOptions,
}

impl TryFrom<options::DatabaseOptionsDeltaLake> for DatabaseOptionsDeltaLake {
    type Error = ProtoConvError;
    fn try_from(value: options::DatabaseOptionsDeltaLake) -> Result<Self, Self::Error> {
        let catalog: DeltaLakeCatalog = value.catalog.required("catalog")?;
        let storage_options: StorageOptions = value.storage_options.required("storage_options")?;
        Ok(DatabaseOptionsDeltaLake {
            catalog,
            storage_options,
        })
    }
}

impl From<DatabaseOptionsDeltaLake> for options::DatabaseOptionsDeltaLake {
    fn from(value: DatabaseOptionsDeltaLake) -> Self {
        options::DatabaseOptionsDeltaLake {
            catalog: Some(value.catalog.into()),
            storage_options: Some(value.storage_options.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DeltaLakeCatalog {
    Unity(DeltaLakeUnityCatalog),
}

impl TryFrom<options::database_options_delta_lake::Catalog> for DeltaLakeCatalog {
    type Error = ProtoConvError;
    fn try_from(value: options::database_options_delta_lake::Catalog) -> Result<Self, Self::Error> {
        Ok(match value {
            options::database_options_delta_lake::Catalog::Unity(v) => {
                DeltaLakeCatalog::Unity(v.try_into()?)
            }
        })
    }
}

impl From<DeltaLakeCatalog> for options::database_options_delta_lake::Catalog {
    fn from(value: DeltaLakeCatalog) -> Self {
        match value {
            DeltaLakeCatalog::Unity(v) => {
                options::database_options_delta_lake::Catalog::Unity(v.into())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeltaLakeUnityCatalog {
    pub catalog_id: String,
    pub databricks_access_token: String,
    pub workspace_url: String,
}

impl TryFrom<options::DeltaLakeUnityCatalog> for DeltaLakeUnityCatalog {
    type Error = ProtoConvError;
    fn try_from(value: options::DeltaLakeUnityCatalog) -> Result<Self, Self::Error> {
        Ok(DeltaLakeUnityCatalog {
            catalog_id: value.catalog_id,
            databricks_access_token: value.databricks_access_token,
            workspace_url: value.workspace_url,
        })
    }
}

impl From<DeltaLakeUnityCatalog> for options::DeltaLakeUnityCatalog {
    fn from(value: DeltaLakeUnityCatalog) -> Self {
        options::DeltaLakeUnityCatalog {
            catalog_id: value.catalog_id,
            databricks_access_token: value.databricks_access_token,
            workspace_url: value.workspace_url,
        }
    }
}

/// Options for a generic `ObjectStore`; to make them as versatile and compact
/// as possible it's just a wrapper for a map, like in `delta-rs`, except here
/// it's a `BTreeMap` instead of a `HashMap`, since the former is `Hash` unlike
/// the latter. This enables us to capture a variety of different (potentially
/// optional) parameters across different object stores and use-cases.
///
/// The following is a list of supported config options in `object_store` crate
/// by object store type:
///
/// - [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants)
/// - [S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants)
/// - [Google options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants)
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct StorageOptions {
    pub inner: BTreeMap<String, String>,
}

impl StorageOptions {
    /// Create a new set of storage options from some iterator of (k, v).
    pub fn new_from_iter<K, V, I>(iter: I) -> Self
    where
        K: Into<String>,
        V: Into<String>,
        I: IntoIterator<Item = (K, V)>,
    {
        let iter = iter.into_iter().map(|(k, v)| (k.into(), v.into()));
        let inner = BTreeMap::from_iter(iter);
        StorageOptions { inner }
    }
}

impl TryFrom<options::StorageOptions> for StorageOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::StorageOptions) -> Result<Self, Self::Error> {
        Ok(StorageOptions { inner: value.inner })
    }
}

impl From<StorageOptions> for options::StorageOptions {
    fn from(value: StorageOptions) -> Self {
        options::StorageOptions { inner: value.inner }
    }
}

// Table options
pub trait TableOptionsImpl: std::fmt::Debug + Send + Sync {
    fn name(&self) -> &'static str;
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptions {
    pub name: String,
    pub options: BTreeMap<String, OptionValue>,
}

impl Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl TableOptions {
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum OptionValue {
    String(String),
    Int(i64),
    Bool(bool),
    Array(Vec<OptionValue>),
    Object(BTreeMap<String, OptionValue>),
    DataType(DataType),
}

impl TryFrom<OptionValue> for String {
    type Error = ProtoConvError;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        match value {
            OptionValue::String(v) => Ok(v),
            _ => Err(ProtoConvError::ParseError("Expected string".to_string())),
        }
    }
}

impl TryFrom<OptionValue> for DataType {
    type Error = ProtoConvError;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        match value {
            OptionValue::DataType(v) => Ok(v),
            _ => Err(ProtoConvError::ParseError("Expected DataType".to_string())),
        }
    }
}

impl TryFrom<OptionValue> for i64 {
    type Error = ProtoConvError;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        match value {
            OptionValue::Int(v) => Ok(v),
            _ => Err(ProtoConvError::ParseError("Expected int".to_string())),
        }
    }
}

impl TryFrom<OptionValue> for bool {
    type Error = ProtoConvError;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        match value {
            OptionValue::Bool(v) => Ok(v),
            _ => Err(ProtoConvError::ParseError("Expected bool".to_string())),
        }
    }
}

impl TryFrom<OptionValue> for Vec<OptionValue> {
    type Error = ProtoConvError;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        match value {
            OptionValue::Array(v) => Ok(v),
            _ => Err(ProtoConvError::ParseError("Expected array".to_string())),
        }
    }
}

impl TryFrom<OptionValue> for BTreeMap<String, OptionValue> {
    type Error = ProtoConvError;

    fn try_from(value: OptionValue) -> Result<Self, Self::Error> {
        match value {
            OptionValue::Object(v) => Ok(v),
            _ => Err(ProtoConvError::ParseError("Expected object".to_string())),
        }
    }
}

impl From<String> for OptionValue {
    fn from(value: String) -> Self {
        OptionValue::String(value)
    }
}
impl From<i64> for OptionValue {
    fn from(value: i64) -> Self {
        OptionValue::Int(value)
    }
}
impl From<bool> for OptionValue {
    fn from(value: bool) -> Self {
        OptionValue::Bool(value)
    }
}

impl<T> From<Vec<T>> for OptionValue
where
    T: Into<OptionValue>,
{
    fn from(value: Vec<T>) -> Self {
        OptionValue::Array(value.into_iter().map(|v| v.into()).collect())
    }
}


impl TryFrom<options::TableOptions> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptions) -> Result<Self, Self::Error> {
        let name = value.name;
        let values = value
            .options
            .into_iter()
            .map(|(k, v)| Ok::<_, ProtoConvError>((k, v.try_into()?)))
            .collect::<Result<_, _>>()?;


        Ok(TableOptions {
            options: values,
            name,
        })
    }
}

impl From<TableOptions> for options::TableOptions {
    fn from(value: TableOptions) -> Self {
        options::TableOptions {
            name: value.name,
            options: value
                .options
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        }
    }
}

impl TryFrom<options::OptionValue> for OptionValue {
    type Error = ProtoConvError;

    fn try_from(value: options::OptionValue) -> Result<Self, Self::Error> {
        let value: options::option_value::Value = value.value.required("value")?;

        Ok(match value {
            options::option_value::Value::StringValue(s) => OptionValue::String(s),
            options::option_value::Value::IntValue(i) => OptionValue::Int(i),
            options::option_value::Value::BoolValue(b) => OptionValue::Bool(b),
            options::option_value::Value::ArrayValue(arr) => OptionValue::Array(
                arr.values
                    .into_iter()
                    .map(|v| v.try_into())
                    .collect::<Result<_, _>>()?,
            ),
            options::option_value::Value::MapValue(map) => OptionValue::Object(
                map.values
                    .into_iter()
                    .map(|(k, v)| Ok::<_, ProtoConvError>((k, v.try_into()?)))
                    .collect::<Result<_, _>>()?,
            ),
            options::option_value::Value::ArrowType(ref dtype) => {
                let arrow_type: DataType = dtype.try_into().map_err(|e| {
                    ProtoConvError::ParseError(format!("Failed to parse arrow type: {}", e))
                })?;
                OptionValue::DataType(arrow_type)
            }
        })
    }
}

impl From<OptionValue> for options::OptionValue {
    fn from(value: OptionValue) -> Self {
        match value {
            OptionValue::String(v) => options::OptionValue {
                value: Some(options::option_value::Value::StringValue(v)),
            },
            OptionValue::Int(v) => options::OptionValue {
                value: Some(options::option_value::Value::IntValue(v)),
            },
            OptionValue::Bool(v) => options::OptionValue {
                value: Some(options::option_value::Value::BoolValue(v)),
            },
            OptionValue::Array(v) => options::OptionValue {
                value: Some(options::option_value::Value::ArrayValue(
                    options::OptionValueArray {
                        values: v.into_iter().map(|v| v.into()).collect(),
                    },
                )),
            },
            OptionValue::Object(v) => options::OptionValue {
                value: Some(options::option_value::Value::MapValue(
                    options::OptionValueMap {
                        values: v
                            .into_iter()
                            .map(|(k, v)| (k, v.into()))
                            .collect::<HashMap<_, _>>(),
                    },
                )),
            },
            OptionValue::DataType(v) => {
                let arrow_type = arrow::ArrowType::try_from(&v).unwrap();

                options::OptionValue {
                    value: Some(options::option_value::Value::ArrowType(arrow_type)),
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TableOptionsOld {
    Internal(TableOptionsInternal),
    Debug(TableOptionsDebug),
    Postgres(TableOptionsPostgres),
    BigQuery(TableOptionsBigQuery),
    Mysql(TableOptionsMysql),
    Local(TableOptionsLocal),
    Gcs(TableOptionsGcs),
    S3(TableOptionsS3),
    MongoDb(TableOptionsMongoDb),
    Snowflake(TableOptionsSnowflake),
    Delta(TableOptionsObjectStore),
    Iceberg(TableOptionsObjectStore),
    Azure(TableOptionsObjectStore),
    SqlServer(TableOptionsSqlServer),
    Lance(TableOptionsObjectStore),
    Bson(TableOptionsObjectStore),
    Clickhouse(TableOptionsClickhouse),
    Cassandra(TableOptionsCassandra),
    Excel(TableOptionsExcel),
    Sqlite(TableOptionsSqlite),
}

impl TableOptionsOld {
    pub const INTERNAL: &'static str = "internal";
    pub const DEBUG: &'static str = "debug";
    pub const POSTGRES: &'static str = "postgres";
    pub const BIGQUERY: &'static str = "bigquery";
    pub const MYSQL: &'static str = "mysql";
    pub const LOCAL: &'static str = "local";
    pub const GCS: &'static str = "gcs";
    pub const S3_STORAGE: &'static str = "s3";
    pub const MONGODB: &'static str = "mongo";
    pub const SNOWFLAKE: &'static str = "snowflake";
    pub const DELTA: &'static str = "delta";
    pub const ICEBERG: &'static str = "iceberg";
    pub const AZURE: &'static str = "azure";
    pub const SQL_SERVER: &'static str = "sql_server";
    pub const LANCE: &'static str = "lance";
    pub const BSON: &'static str = "bson";
    pub const CLICKHOUSE: &'static str = "clickhouse";
    pub const CASSANDRA: &'static str = "cassandra";
    pub const EXCEL: &'static str = "excel";
    pub const SQLITE: &'static str = "sqlite";

    pub const fn new_internal(columns: Vec<InternalColumnDefinition>) -> TableOptionsOld {
        TableOptionsOld::Internal(TableOptionsInternal { columns })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TableOptionsOld::Internal(_) => Self::INTERNAL,
            TableOptionsOld::Debug(_) => Self::DEBUG,
            TableOptionsOld::Postgres(_) => Self::POSTGRES,
            TableOptionsOld::BigQuery(_) => Self::BIGQUERY,
            TableOptionsOld::Mysql(_) => Self::MYSQL,
            TableOptionsOld::Local(_) => Self::LOCAL,
            TableOptionsOld::Gcs(_) => Self::GCS,
            TableOptionsOld::S3(_) => Self::S3_STORAGE,
            TableOptionsOld::MongoDb(_) => Self::MONGODB,
            TableOptionsOld::Snowflake(_) => Self::SNOWFLAKE,
            TableOptionsOld::Delta(_) => Self::DELTA,
            TableOptionsOld::Iceberg(_) => Self::ICEBERG,
            TableOptionsOld::Azure(_) => Self::AZURE,
            TableOptionsOld::SqlServer(_) => Self::SQL_SERVER,
            TableOptionsOld::Lance(_) => Self::LANCE,
            TableOptionsOld::Bson(_) => Self::BSON,
            TableOptionsOld::Clickhouse(_) => Self::CLICKHOUSE,
            TableOptionsOld::Cassandra(_) => Self::CASSANDRA,
            TableOptionsOld::Excel(_) => Self::EXCEL,
            TableOptionsOld::Sqlite(_) => Self::SQLITE,
        }
    }
}

impl fmt::Display for TableOptionsOld {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<TableOptionsOld> for options::table_options_old::Options {
    type Error = ProtoConvError;
    fn try_from(value: TableOptionsOld) -> Result<Self, Self::Error> {
        Ok(match value {
            TableOptionsOld::Internal(v) => {
                options::table_options_old::Options::Internal(v.try_into()?)
            }
            TableOptionsOld::Debug(v) => options::table_options_old::Options::Debug(v.into()),
            TableOptionsOld::Postgres(v) => options::table_options_old::Options::Postgres(v.into()),
            TableOptionsOld::BigQuery(v) => options::table_options_old::Options::Bigquery(v.into()),
            TableOptionsOld::Mysql(v) => options::table_options_old::Options::Mysql(v.into()),
            TableOptionsOld::Local(v) => options::table_options_old::Options::Local(v.into()),
            TableOptionsOld::Gcs(v) => options::table_options_old::Options::Gcs(v.into()),
            TableOptionsOld::S3(v) => options::table_options_old::Options::S3(v.into()),
            TableOptionsOld::MongoDb(v) => options::table_options_old::Options::Mongo(v.into()),
            TableOptionsOld::Snowflake(v) => {
                options::table_options_old::Options::Snowflake(v.into())
            }
            TableOptionsOld::Delta(v) => options::table_options_old::Options::Delta(v.into()),
            TableOptionsOld::Iceberg(v) => options::table_options_old::Options::Iceberg(v.into()),
            TableOptionsOld::Azure(v) => options::table_options_old::Options::Azure(v.into()),
            TableOptionsOld::SqlServer(v) => {
                options::table_options_old::Options::SqlServer(v.into())
            }
            TableOptionsOld::Lance(v) => options::table_options_old::Options::Lance(v.into()),
            TableOptionsOld::Bson(v) => options::table_options_old::Options::Bson(v.into()),
            TableOptionsOld::Clickhouse(v) => {
                options::table_options_old::Options::Clickhouse(v.into())
            }
            TableOptionsOld::Cassandra(v) => {
                options::table_options_old::Options::Cassandra(v.into())
            }
            TableOptionsOld::Excel(v) => options::table_options_old::Options::Excel(v.into()),
            TableOptionsOld::Sqlite(v) => options::table_options_old::Options::Sqlite(v.into()),
        })
    }
}

impl TryFrom<TableOptionsOld> for options::TableOptionsOld {
    type Error = ProtoConvError;
    fn try_from(value: TableOptionsOld) -> Result<Self, Self::Error> {
        Ok(options::TableOptionsOld {
            options: Some(value.try_into()?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsInternal {
    pub columns: Vec<InternalColumnDefinition>,
}

impl From<TableOptionsInternal> for TableOptions {
    fn from(value: TableOptionsInternal) -> Self {
        let mut options = BTreeMap::new();
        let columns = value.columns.into_iter().map(|col| col.into());
        options.insert("columns".to_string(), OptionValue::Array(columns.collect()));

        TableOptions {
            name: "internal".to_string(),
            options,
        }
    }
}

impl TryFrom<TableOptions> for TableOptionsInternal {
    type Error = ProtoConvError;
    fn try_from(value: TableOptions) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&TableOptions> for TableOptionsInternal {
    type Error = ProtoConvError;
    fn try_from(value: &TableOptions) -> Result<Self, Self::Error> {
        if matches!(value.name.as_ref(), "internal") {
            let columns: Vec<OptionValue> = value
                .options
                .get("columns")
                .cloned()
                .ok_or_else(|| ProtoConvError::RequiredField("columns".to_string()))?
                .try_into()?;

            let columns = columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?;


            Ok(TableOptionsInternal { columns })
        } else {
            Err(ProtoConvError::UnknownVariant(value.name.to_string()))
        }
    }
}


impl From<DFSchemaRef> for TableOptionsInternal {
    fn from(value: DFSchemaRef) -> Self {
        TableOptionsInternal {
            columns: value
                .fields()
                .iter()
                .map(|col| InternalColumnDefinition {
                    name: col.name().clone(),
                    nullable: col.is_nullable(),
                    arrow_type: col.data_type().clone(),
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl From<SchemaRef> for TableOptionsInternal {
    fn from(value: SchemaRef) -> Self {
        TableOptionsInternal {
            columns: value
                .fields()
                .iter()
                .map(|col| InternalColumnDefinition {
                    name: col.name().clone(),
                    nullable: col.is_nullable(),
                    arrow_type: col.data_type().clone(),
                })
                .collect::<Vec<_>>(),
        }
    }
}

impl TryFrom<options::TableOptionsInternal> for TableOptionsInternal {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsInternal) -> Result<Self, Self::Error> {
        Ok(TableOptionsInternal {
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<TableOptionsInternal> for options::TableOptionsInternal {
    type Error = ProtoConvError;
    fn try_from(value: TableOptionsInternal) -> Result<Self, Self::Error> {
        Ok(options::TableOptionsInternal {
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsDebug {
    pub table_type: String,
}
impl From<TableOptionsDebug> for TableOptions {
    fn from(value: TableOptionsDebug) -> Self {
        let mut options = BTreeMap::new();
        options.insert(
            "table_type".to_string(),
            OptionValue::String(value.table_type),
        );

        TableOptions {
            name: "debug".to_string(),
            options,
        }
    }
}

impl TryFrom<options::TableOptionsDebug> for TableOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsDebug) -> Result<Self, Self::Error> {
        Ok(TableOptionsDebug {
            table_type: value.table_type,
        })
    }
}

impl From<TableOptionsDebug> for options::TableOptionsDebug {
    fn from(value: TableOptionsDebug) -> Self {
        options::TableOptionsDebug {
            table_type: value.table_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsPostgres {
    pub connection_string: String,
    pub schema: String,
    pub table: String,
}

impl TryFrom<options::TableOptionsPostgres> for TableOptionsPostgres {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsPostgres) -> Result<Self, Self::Error> {
        Ok(TableOptionsPostgres {
            connection_string: value.connection_string,
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<TableOptionsPostgres> for options::TableOptionsPostgres {
    fn from(value: TableOptionsPostgres) -> Self {
        options::TableOptionsPostgres {
            connection_string: value.connection_string,
            schema: value.schema,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsBigQuery {
    pub service_account_key: String,
    pub project_id: String,
    pub dataset_id: String,
    pub table_id: String,
}

impl TryFrom<options::TableOptionsBigQuery> for TableOptionsBigQuery {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsBigQuery) -> Result<Self, Self::Error> {
        Ok(TableOptionsBigQuery {
            service_account_key: value.service_account_key,
            project_id: value.project_id,
            dataset_id: value.dataset_id,
            table_id: value.table_id,
        })
    }
}

impl From<TableOptionsBigQuery> for options::TableOptionsBigQuery {
    fn from(value: TableOptionsBigQuery) -> Self {
        options::TableOptionsBigQuery {
            service_account_key: value.service_account_key,
            project_id: value.project_id,
            dataset_id: value.dataset_id,
            table_id: value.table_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsMysql {
    pub connection_string: String,
    pub schema: String,
    pub table: String,
}

impl TryFrom<options::TableOptionsMysql> for TableOptionsMysql {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsMysql) -> Result<Self, Self::Error> {
        Ok(TableOptionsMysql {
            connection_string: value.connection_string,
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<TableOptionsMysql> for options::TableOptionsMysql {
    fn from(value: TableOptionsMysql) -> Self {
        options::TableOptionsMysql {
            connection_string: value.connection_string,
            schema: value.schema,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsLocal {
    pub location: String,
    pub file_type: String,
    pub compression: Option<String>,
}

impl TryFrom<options::TableOptionsLocal> for TableOptionsLocal {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsLocal) -> Result<Self, Self::Error> {
        Ok(TableOptionsLocal {
            location: value.location,
            file_type: value.file_type,
            compression: value.compression,
        })
    }
}

impl From<TableOptionsLocal> for options::TableOptionsLocal {
    fn from(value: TableOptionsLocal) -> Self {
        options::TableOptionsLocal {
            location: value.location,
            file_type: value.file_type,
            compression: value.compression,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsGcs {
    pub service_account_key: Option<String>,
    pub bucket: String,
    pub location: String,
    pub file_type: String,
    pub compression: Option<String>,
}

impl TryFrom<options::TableOptionsGcs> for TableOptionsGcs {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsGcs) -> Result<Self, Self::Error> {
        Ok(TableOptionsGcs {
            service_account_key: value.service_account_key,
            bucket: value.bucket,
            location: value.location,
            file_type: value.file_type,
            compression: value.compression,
        })
    }
}

impl From<TableOptionsGcs> for options::TableOptionsGcs {
    fn from(value: TableOptionsGcs) -> Self {
        options::TableOptionsGcs {
            service_account_key: value.service_account_key,
            bucket: value.bucket,
            location: value.location,
            file_type: value.file_type,
            compression: value.compression,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsS3 {
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub bucket: String,
    pub location: String,
    pub file_type: String,
    pub compression: Option<String>,
}

impl TryFrom<options::TableOptionsS3> for TableOptionsS3 {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsS3) -> Result<Self, Self::Error> {
        Ok(TableOptionsS3 {
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
            region: value.region,
            bucket: value.bucket,
            location: value.location,
            file_type: value.file_type,
            compression: value.compression,
        })
    }
}

impl From<TableOptionsS3> for options::TableOptionsS3 {
    fn from(value: TableOptionsS3) -> Self {
        options::TableOptionsS3 {
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
            region: value.region,
            bucket: value.bucket,
            location: value.location,
            file_type: value.file_type,
            compression: value.compression,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsMongoDb {
    pub connection_string: String,
    pub database: String,
    pub collection: String,
}

impl TryFrom<options::TableOptionsMongo> for TableOptionsMongoDb {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsMongo) -> Result<Self, Self::Error> {
        Ok(TableOptionsMongoDb {
            connection_string: value.connection_string,
            database: value.database,
            collection: value.collection,
        })
    }
}

impl From<TableOptionsMongoDb> for options::TableOptionsMongo {
    fn from(value: TableOptionsMongoDb) -> Self {
        options::TableOptionsMongo {
            connection_string: value.connection_string,
            database: value.database,
            collection: value.collection,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsExcel {
    pub location: String,
    pub storage_options: StorageOptions,
    pub file_type: Option<String>,
    pub compression: Option<String>,
    pub sheet_name: Option<String>,
    pub has_header: bool,
}

impl TryFrom<options::TableOptionsExcel> for TableOptionsExcel {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsExcel) -> Result<Self, Self::Error> {
        Ok(TableOptionsExcel {
            location: value.location,
            storage_options: value.storage_options.required("storage_options")?,
            file_type: value.file_type,
            compression: value.compression,
            sheet_name: value.sheet_name,
            has_header: value.has_header,
        })
    }
}

impl From<TableOptionsExcel> for options::TableOptionsExcel {
    fn from(value: TableOptionsExcel) -> Self {
        options::TableOptionsExcel {
            location: value.location,
            storage_options: Some(value.storage_options.into()),
            file_type: value.file_type,
            compression: value.compression,
            sheet_name: value.sheet_name,
            has_header: value.has_header,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsSqlServer {
    pub connection_string: String,
    pub schema: String,
    pub table: String,
}

impl TryFrom<options::TableOptionsSqlServer> for TableOptionsSqlServer {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsSqlServer) -> Result<Self, Self::Error> {
        Ok(TableOptionsSqlServer {
            connection_string: value.connection_string,
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<TableOptionsSqlServer> for options::TableOptionsSqlServer {
    fn from(value: TableOptionsSqlServer) -> Self {
        options::TableOptionsSqlServer {
            connection_string: value.connection_string,
            schema: value.schema,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsClickhouse {
    pub connection_string: String,
    pub table: String,
    pub database: Option<String>,
}

impl TryFrom<options::TableOptionsClickhouse> for TableOptionsClickhouse {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsClickhouse) -> Result<Self, Self::Error> {
        Ok(TableOptionsClickhouse {
            connection_string: value.connection_string,
            table: value.table,
            database: value.database,
        })
    }
}

impl From<TableOptionsClickhouse> for options::TableOptionsClickhouse {
    fn from(value: TableOptionsClickhouse) -> Self {
        options::TableOptionsClickhouse {
            connection_string: value.connection_string,
            table: value.table,
            database: value.database,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsCassandra {
    pub host: String,
    pub keyspace: String,
    pub table: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl TryFrom<options::TableOptionsCassandra> for TableOptionsCassandra {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsCassandra) -> Result<Self, Self::Error> {
        Ok(TableOptionsCassandra {
            host: value.host,
            keyspace: value.keyspace,
            table: value.table,
            username: value.username,
            password: value.password,
        })
    }
}

impl From<TableOptionsCassandra> for options::TableOptionsCassandra {
    fn from(value: TableOptionsCassandra) -> Self {
        options::TableOptionsCassandra {
            host: value.host,
            keyspace: value.keyspace,
            table: value.table,
            username: value.username,
            password: value.password,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsSqlite {
    pub location: String,
    pub table: String,
}

impl TryFrom<options::TableOptionsSqlite> for TableOptionsSqlite {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsSqlite) -> Result<Self, Self::Error> {
        Ok(TableOptionsSqlite {
            location: value.location,
            table: value.table,
        })
    }
}

impl From<TableOptionsSqlite> for options::TableOptionsSqlite {
    fn from(value: TableOptionsSqlite) -> Self {
        options::TableOptionsSqlite {
            location: value.location,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsSnowflake {
    pub account_name: String,
    pub login_name: String,
    pub password: String,
    pub database_name: String,
    pub warehouse: String,
    pub role_name: String,
    pub schema_name: String,
    pub table_name: String,
}

impl TryFrom<options::TableOptionsSnowflake> for TableOptionsSnowflake {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsSnowflake) -> Result<Self, Self::Error> {
        Ok(TableOptionsSnowflake {
            account_name: value.account_name,
            login_name: value.login_name,
            password: value.password,
            database_name: value.database_name,
            warehouse: value.warehouse,
            role_name: value.role_name,
            schema_name: value.schema_name,
            table_name: value.table_name,
        })
    }
}

impl From<TableOptionsSnowflake> for options::TableOptionsSnowflake {
    fn from(value: TableOptionsSnowflake) -> Self {
        options::TableOptionsSnowflake {
            account_name: value.account_name,
            login_name: value.login_name,
            password: value.password,
            database_name: value.database_name,
            warehouse: value.warehouse,
            role_name: value.role_name,
            schema_name: value.schema_name,
            table_name: value.table_name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableOptionsObjectStore {
    pub location: String,
    pub storage_options: StorageOptions,
    pub file_type: Option<String>,
    pub compression: Option<String>,
    pub schema_sample_size: Option<i64>,
}

impl TryFrom<options::TableOptionsObjectStore> for TableOptionsObjectStore {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsObjectStore) -> Result<Self, Self::Error> {
        Ok(TableOptionsObjectStore {
            location: value.location,
            storage_options: value.storage_options.required("storage_options")?,
            file_type: value.file_type,
            compression: value.compression,
            schema_sample_size: value.schema_sample_size,
        })
    }
}

impl From<TableOptionsObjectStore> for options::TableOptionsObjectStore {
    fn from(value: TableOptionsObjectStore) -> Self {
        options::TableOptionsObjectStore {
            location: value.location,
            storage_options: Some(value.storage_options.into()),
            file_type: value.file_type,
            compression: value.compression,
            schema_sample_size: value.schema_sample_size,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TunnelOptions {
    Internal(TunnelOptionsInternal),
    Debug(TunnelOptionsDebug),
    Ssh(TunnelOptionsSsh),
}

impl TunnelOptions {
    pub const INTERNAL: &'static str = "internal";
    pub const DEBUG: &'static str = "debug";
    pub const SSH: &'static str = "ssh";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Internal(_) => Self::INTERNAL,
            Self::Debug(_) => Self::DEBUG,
            Self::Ssh(_) => Self::SSH,
        }
    }
}

impl fmt::Display for TunnelOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<options::tunnel_options::Options> for TunnelOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::tunnel_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            options::tunnel_options::Options::Internal(v) => Self::Internal(v.try_into()?),
            options::tunnel_options::Options::Debug(v) => Self::Debug(v.try_into()?),
            options::tunnel_options::Options::Ssh(v) => Self::Ssh(v.try_into()?),
        })
    }
}

impl TryFrom<options::TunnelOptions> for TunnelOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::TunnelOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<TunnelOptions> for options::tunnel_options::Options {
    fn from(value: TunnelOptions) -> Self {
        match value {
            TunnelOptions::Internal(v) => options::tunnel_options::Options::Internal(v.into()),
            TunnelOptions::Debug(v) => options::tunnel_options::Options::Debug(v.into()),
            TunnelOptions::Ssh(v) => options::tunnel_options::Options::Ssh(v.into()),
        }
    }
}

impl From<TunnelOptions> for options::TunnelOptions {
    fn from(value: TunnelOptions) -> Self {
        options::TunnelOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TunnelOptionsInternal {}

impl TryFrom<options::TunnelOptionsInternal> for TunnelOptionsInternal {
    type Error = ProtoConvError;
    fn try_from(_value: options::TunnelOptionsInternal) -> Result<Self, Self::Error> {
        Ok(TunnelOptionsInternal {})
    }
}

impl From<TunnelOptionsInternal> for options::TunnelOptionsInternal {
    fn from(_value: TunnelOptionsInternal) -> Self {
        options::TunnelOptionsInternal {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TunnelOptionsDebug {}

impl TryFrom<options::TunnelOptionsDebug> for TunnelOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(_value: options::TunnelOptionsDebug) -> Result<Self, Self::Error> {
        Ok(TunnelOptionsDebug {})
    }
}

impl From<TunnelOptionsDebug> for options::TunnelOptionsDebug {
    fn from(_value: TunnelOptionsDebug) -> Self {
        options::TunnelOptionsDebug {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TunnelOptionsSsh {
    pub connection_string: String,
    pub ssh_key: Vec<u8>,
}

impl TryFrom<options::TunnelOptionsSsh> for TunnelOptionsSsh {
    type Error = ProtoConvError;
    fn try_from(value: options::TunnelOptionsSsh) -> Result<Self, Self::Error> {
        Ok(TunnelOptionsSsh {
            connection_string: value.connection_string,
            ssh_key: value.ssh_key,
        })
    }
}

impl From<TunnelOptionsSsh> for options::TunnelOptionsSsh {
    fn from(value: TunnelOptionsSsh) -> Self {
        options::TunnelOptionsSsh {
            connection_string: value.connection_string,
            ssh_key: value.ssh_key,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CredentialsOptions {
    Debug(CredentialsOptionsDebug),
    Gcp(CredentialsOptionsGcp),
    Aws(CredentialsOptionsAws),
    Azure(CredentialsOptionsAzure),
    OpenAI(CredentialsOptionsOpenAI),
}

impl CredentialsOptions {
    pub const DEBUG: &'static str = "debug";
    pub const GCP: &'static str = "gcp";
    pub const AWS: &'static str = "aws";
    pub const AZURE: &'static str = "azure";
    pub const OPENAI: &'static str = "openai";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Debug(_) => Self::DEBUG,
            Self::Gcp(_) => Self::GCP,
            Self::Aws(_) => Self::AWS,
            Self::Azure(_) => Self::AZURE,
            Self::OpenAI(_) => Self::OPENAI,
        }
    }
}

impl fmt::Display for CredentialsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<options::credentials_options::Options> for CredentialsOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::credentials_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            options::credentials_options::Options::Debug(v) => Self::Debug(v.try_into()?),
            options::credentials_options::Options::Gcp(v) => Self::Gcp(v.try_into()?),
            options::credentials_options::Options::Aws(v) => Self::Aws(v.try_into()?),
            options::credentials_options::Options::Azure(v) => Self::Azure(v.try_into()?),
            options::credentials_options::Options::Openai(v) => Self::OpenAI(v.try_into()?),
        })
    }
}

impl TryFrom<options::CredentialsOptions> for CredentialsOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::CredentialsOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<CredentialsOptions> for options::credentials_options::Options {
    fn from(value: CredentialsOptions) -> Self {
        match value {
            CredentialsOptions::Debug(v) => options::credentials_options::Options::Debug(v.into()),
            CredentialsOptions::Gcp(v) => options::credentials_options::Options::Gcp(v.into()),
            CredentialsOptions::Aws(v) => options::credentials_options::Options::Aws(v.into()),
            CredentialsOptions::Azure(v) => options::credentials_options::Options::Azure(v.into()),
            CredentialsOptions::OpenAI(v) => {
                options::credentials_options::Options::Openai(v.into())
            }
        }
    }
}

impl From<CredentialsOptions> for options::CredentialsOptions {
    fn from(value: CredentialsOptions) -> Self {
        options::CredentialsOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialsOptionsDebug {
    pub table_type: String,
}

impl TryFrom<options::CredentialsOptionsDebug> for CredentialsOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(value: options::CredentialsOptionsDebug) -> Result<Self, Self::Error> {
        Ok(CredentialsOptionsDebug {
            table_type: value.table_type,
        })
    }
}

impl From<CredentialsOptionsDebug> for options::CredentialsOptionsDebug {
    fn from(value: CredentialsOptionsDebug) -> Self {
        options::CredentialsOptionsDebug {
            table_type: value.table_type,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialsOptionsGcp {
    pub service_account_key: String,
}

impl TryFrom<options::CredentialsOptionsGcp> for CredentialsOptionsGcp {
    type Error = ProtoConvError;
    fn try_from(value: options::CredentialsOptionsGcp) -> Result<Self, Self::Error> {
        Ok(CredentialsOptionsGcp {
            service_account_key: value.service_account_key,
        })
    }
}

impl From<CredentialsOptionsGcp> for options::CredentialsOptionsGcp {
    fn from(value: CredentialsOptionsGcp) -> Self {
        options::CredentialsOptionsGcp {
            service_account_key: value.service_account_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialsOptionsAws {
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl TryFrom<options::CredentialsOptionsAws> for CredentialsOptionsAws {
    type Error = ProtoConvError;
    fn try_from(value: options::CredentialsOptionsAws) -> Result<Self, Self::Error> {
        Ok(CredentialsOptionsAws {
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
        })
    }
}

impl From<CredentialsOptionsAws> for options::CredentialsOptionsAws {
    fn from(value: CredentialsOptionsAws) -> Self {
        options::CredentialsOptionsAws {
            access_key_id: value.access_key_id,
            secret_access_key: value.secret_access_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialsOptionsAzure {
    pub account_name: String,
    pub access_key: String,
}

impl TryFrom<options::CredentialsOptionsAzure> for CredentialsOptionsAzure {
    type Error = ProtoConvError;
    fn try_from(value: options::CredentialsOptionsAzure) -> Result<Self, Self::Error> {
        Ok(CredentialsOptionsAzure {
            account_name: value.account_name,
            access_key: value.access_key,
        })
    }
}

impl From<CredentialsOptionsAzure> for options::CredentialsOptionsAzure {
    fn from(value: CredentialsOptionsAzure) -> Self {
        options::CredentialsOptionsAzure {
            account_name: value.account_name,
            access_key: value.access_key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CredentialsOptionsOpenAI {
    pub api_key: String,
    pub api_base: Option<String>,
    pub org_id: Option<String>,
}

impl CredentialsOptionsOpenAI {
    pub fn fields() -> Fields {
        vec![
            Field::new("api_key", DataType::Utf8, false),
            Field::new("api_base", DataType::Utf8, true),
            Field::new("org_id", DataType::Utf8, true),
        ]
        .into()
    }
    pub fn data_type() -> DataType {
        DataType::Struct(Self::fields())
    }
}
impl From<CredentialsOptionsOpenAI> for datafusion::scalar::ScalarValue {
    fn from(value: CredentialsOptionsOpenAI) -> Self {
        datafusion::scalar::ScalarValue::Struct(
            Some(vec![
                datafusion::scalar::ScalarValue::Utf8(Some(value.api_key)),
                datafusion::scalar::ScalarValue::Utf8(value.api_base),
                datafusion::scalar::ScalarValue::Utf8(value.org_id),
            ]),
            CredentialsOptionsOpenAI::fields(),
        )
    }
}

impl TryFrom<options::CredentialsOptionsOpenAi> for CredentialsOptionsOpenAI {
    type Error = ProtoConvError;
    fn try_from(value: options::CredentialsOptionsOpenAi) -> Result<Self, Self::Error> {
        Ok(CredentialsOptionsOpenAI {
            api_key: value.api_key,
            api_base: value.api_base,
            org_id: value.org_id,
        })
    }
}

impl From<CredentialsOptionsOpenAI> for options::CredentialsOptionsOpenAi {
    fn from(value: CredentialsOptionsOpenAI) -> Self {
        options::CredentialsOptionsOpenAi {
            api_key: value.api_key,
            api_base: value.api_base,
            org_id: value.org_id,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum CopyToDestinationOptions {
    Local(CopyToDestinationOptionsLocal),
    Gcs(CopyToDestinationOptionsGcs),
    S3(CopyToDestinationOptionsS3),
    Azure(CopyToDestinationOptionsAzure),
}

impl CopyToDestinationOptions {
    pub const LOCAL: &'static str = "local";
    pub const GCS: &'static str = "gcs";
    pub const S3_STORAGE: &'static str = "s3";
    pub const AZURE: &'static str = "azure";

    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Local(_) => Self::LOCAL,
            Self::Gcs(_) => Self::GCS,
            Self::S3(_) => Self::S3_STORAGE,
            Self::Azure(_) => Self::AZURE,
        }
    }

    pub fn location(&self) -> &str {
        match self {
            Self::Local(CopyToDestinationOptionsLocal { location }) => location,
            Self::Gcs(CopyToDestinationOptionsGcs { location, .. }) => location,
            Self::S3(CopyToDestinationOptionsS3 { location, .. }) => location,
            Self::Azure(CopyToDestinationOptionsAzure { location, .. }) => location,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToDestinationOptionsLocal {
    pub location: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToDestinationOptionsGcs {
    pub service_account_key: Option<String>,
    pub bucket: String,
    pub location: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToDestinationOptionsS3 {
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub bucket: String,
    pub location: String,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct CopyToDestinationOptionsAzure {
    pub account: String,
    pub access_key: String,
    pub container: String,
    pub location: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum CopyToFormatOptions {
    Csv(CopyToFormatOptionsCsv),
    Parquet(CopyToFormatOptionsParquet),
    Lance(CopyToFormatOptionsLance),
    Json(CopyToFormatOptionsJson),
    Bson(CopyToFormatOptionsBson),
}

impl Default for CopyToFormatOptions {
    fn default() -> Self {
        Self::Csv(CopyToFormatOptionsCsv {
            delim: b',',
            header: true,
        })
    }
}

impl CopyToFormatOptions {
    pub const CSV: &'static str = "csv";
    pub const PARQUET: &'static str = "parquet";
    pub const JSON: &'static str = "json";
    pub const BSON: &'static str = "bson";
    pub const LANCE: &'static str = "lance";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Csv(_) => Self::CSV,
            Self::Parquet(_) => Self::PARQUET,
            Self::Json(_) => Self::JSON,
            Self::Bson(_) => Self::BSON,
            Self::Lance(_) => Self::LANCE,
        }
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToFormatOptionsCsv {
    pub delim: u8,
    pub header: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToFormatOptionsParquet {
    pub row_group_size: usize,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToFormatOptionsJson {
    pub array: bool,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToFormatOptionsBson {}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CopyToFormatOptionsLance {
    pub max_rows_per_file: Option<usize>,
    pub max_rows_per_group: Option<usize>,
    pub max_bytes_per_file: Option<usize>,
    pub input_batch_size: Option<usize>,
}
