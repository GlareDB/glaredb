use std::collections::BTreeMap;
use std::fmt::{self, Display};
use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Fields, SchemaRef};
use datafusion::common::DFSchemaRef;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::gen::common::arrow;
use crate::gen::metastore::options;
use crate::{FromOptionalField, ProtoConvError};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InternalColumnDefinition {
    pub name: String,
    pub nullable: bool,
    pub arrow_type: DataType,
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

    /// Create an iterator of column definitions from arrow fields.
    pub fn from_arrow_fields(
        fields: &Fields,
    ) -> impl Iterator<Item = InternalColumnDefinition> + '_ {
        fields.into_iter().map(|field| InternalColumnDefinition {
            name: field.name().clone(),
            nullable: field.is_nullable(),
            arrow_type: field.data_type().clone(),
        })
    }

    /// Create a vec of column definitions from arrow fields.
    pub fn to_arrow_fields<C>(cols: C) -> Vec<Arc<Field>>
    where
        C: IntoIterator<Item = InternalColumnDefinition>,
    {
        cols.into_iter().map(|col| Arc::new(col.into())).collect()
    }
}

impl From<InternalColumnDefinition> for Field {
    fn from(value: InternalColumnDefinition) -> Self {
        Field::new(value.name, value.arrow_type, value.nullable)
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

impl From<InternalColumnDefinition> for options::InternalColumnDefinition {
    fn from(value: InternalColumnDefinition) -> Self {
        // We don't support any of the unserializable arrow types
        // So this should always be infallible
        let arrow_type =
            arrow::ArrowType::try_from(&value.arrow_type).expect("Arrow type must be serializable");
        options::InternalColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type: Some(arrow_type),
        }
    }
}

// Database options

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorageOptions {
    #[serde(flatten)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsV1 {
    pub name: String,
    pub options: Vec<u8>,
    pub version: u32,
}

pub trait TableOptionsImpl: Serialize + DeserializeOwned {
    const NAME: &'static str;
    const VERSION: u32 = 1;
}

impl Display for TableOptionsV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl TableOptionsV1 {
    pub fn as_str(&self) -> &str {
        &self.name
    }

    pub fn new<S: TableOptionsImpl>(options: &S) -> Self {
        let options = serde_json::to_vec(options).expect("options must serialize");

        TableOptionsV1 {
            name: S::NAME.to_string(),
            options,
            version: S::VERSION,
        }
    }

    pub fn extract<T: TableOptionsImpl>(&self) -> Result<T, ProtoConvError> {
        if self.name != T::NAME {
            return Err(ProtoConvError::ParseError(format!(
                "Expected table options of type {}, got {}",
                T::NAME,
                self.name
            )));
        }
        serde_json::from_slice(&self.options).map_err(|e| ProtoConvError::ParseError(e.to_string()))
    }

    /// Extract the table options to a specific type.
    /// This does not check the variant of the table options, so this will panic if the variant is not the expected one.
    /// Use `extract` if you want to handle the error case.
    /// This should only be used if the variant is checked elsewhere.
    pub fn extract_unchecked<T: TableOptionsImpl>(&self) -> T {
        serde_json::from_slice(&self.options)
            .expect("Options should infallibly deserialize. This indicates a programming error.")
    }
}

impl<T> From<T> for TableOptionsV1
where
    T: TableOptionsImpl,
{
    fn from(value: T) -> Self {
        TableOptionsV1::new(&value)
    }
}

impl TryFrom<options::TableOptionsV1> for TableOptionsV1 {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsV1) -> Result<Self, Self::Error> {
        let name = value.name;
        let values = value.options;

        Ok(TableOptionsV1 {
            options: values,
            name,
            version: value.version,
        })
    }
}

impl From<TableOptionsV1> for options::TableOptionsV1 {
    fn from(value: TableOptionsV1) -> Self {
        options::TableOptionsV1 {
            name: value.name,
            options: value.options,
            version: value.version,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableOptionsV0 {
    Debug(TableOptionsDebug),
    Internal(TableOptionsInternal),
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

impl TableOptionsV0 {
    pub const DEBUG: &'static str = "debug";
    pub const INTERNAL: &'static str = "internal";
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

    pub const fn new_internal(columns: Vec<InternalColumnDefinition>) -> TableOptionsV0 {
        TableOptionsV0::Internal(TableOptionsInternal { columns })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TableOptionsV0::Debug(_) => Self::DEBUG,
            TableOptionsV0::Internal(_) => Self::INTERNAL,
            TableOptionsV0::Postgres(_) => Self::POSTGRES,
            TableOptionsV0::BigQuery(_) => Self::BIGQUERY,
            TableOptionsV0::Mysql(_) => Self::MYSQL,
            TableOptionsV0::Local(_) => Self::LOCAL,
            TableOptionsV0::Gcs(_) => Self::GCS,
            TableOptionsV0::S3(_) => Self::S3_STORAGE,
            TableOptionsV0::MongoDb(_) => Self::MONGODB,
            TableOptionsV0::Snowflake(_) => Self::SNOWFLAKE,
            TableOptionsV0::Delta(_) => Self::DELTA,
            TableOptionsV0::Iceberg(_) => Self::ICEBERG,
            TableOptionsV0::Azure(_) => Self::AZURE,
            TableOptionsV0::SqlServer(_) => Self::SQL_SERVER,
            TableOptionsV0::Lance(_) => Self::LANCE,
            TableOptionsV0::Bson(_) => Self::BSON,
            TableOptionsV0::Clickhouse(_) => Self::CLICKHOUSE,
            TableOptionsV0::Cassandra(_) => Self::CASSANDRA,
            TableOptionsV0::Excel(_) => Self::EXCEL,
            TableOptionsV0::Sqlite(_) => Self::SQLITE,
        }
    }
}

impl From<TableOptionsV0> for TableOptionsV1 {
    fn from(value: TableOptionsV0) -> Self {
        match value {
            TableOptionsV0::Debug(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Internal(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Postgres(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::BigQuery(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Mysql(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Local(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Gcs(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::S3(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::MongoDb(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Snowflake(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Delta(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Iceberg(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Azure(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::SqlServer(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Lance(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Bson(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Clickhouse(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Cassandra(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Excel(opts) => TableOptionsV1::new(&opts),
            TableOptionsV0::Sqlite(opts) => TableOptionsV1::new(&opts),
        }
    }
}

impl TryFrom<&TableOptionsV1> for TableOptionsV0 {
    type Error = ProtoConvError;

    fn try_from(value: &TableOptionsV1) -> Result<Self, Self::Error> {
        let _v: serde_json::Value = serde_json::from_slice(&value.options).unwrap();

        match value.name.as_ref() {
            TableOptionsObjectStore::NAME => {
                let obj_store: TableOptionsObjectStore = value.extract()?;
                let file_type = obj_store.file_type.clone().unwrap_or_default();
                match file_type.as_ref() {
                    Self::DELTA => Ok(TableOptionsV0::Delta(obj_store)),
                    Self::ICEBERG => Ok(TableOptionsV0::Iceberg(obj_store)),
                    Self::AZURE => Ok(TableOptionsV0::Azure(obj_store)),
                    Self::LANCE => Ok(TableOptionsV0::Lance(obj_store)),
                    Self::BSON => Ok(TableOptionsV0::Bson(obj_store)),
                    _ => Err(ProtoConvError::UnknownVariant(value.name.to_string())),
                }
            }
            Self::INTERNAL => {
                let internal: TableOptionsInternal = value.extract()?;
                Ok(TableOptionsV0::Internal(internal))
            }
            Self::POSTGRES => {
                let postgres: TableOptionsPostgres = value.extract()?;
                Ok(TableOptionsV0::Postgres(postgres))
            }
            Self::BIGQUERY => {
                let bigquery: TableOptionsBigQuery = value.extract()?;
                Ok(TableOptionsV0::BigQuery(bigquery))
            }
            Self::MYSQL => {
                let mysql: TableOptionsMysql = value.extract()?;
                Ok(TableOptionsV0::Mysql(mysql))
            }
            Self::LOCAL => {
                let local: TableOptionsLocal = value.extract()?;
                Ok(TableOptionsV0::Local(local))
            }
            Self::GCS => {
                let gcs: TableOptionsGcs = value.extract()?;
                Ok(TableOptionsV0::Gcs(gcs))
            }
            Self::S3_STORAGE => {
                let s3: TableOptionsS3 = value.extract()?;
                Ok(TableOptionsV0::S3(s3))
            }
            Self::MONGODB => {
                let mongo: TableOptionsMongoDb = value.extract()?;
                Ok(TableOptionsV0::MongoDb(mongo))
            }
            Self::SNOWFLAKE => {
                let snowflake: TableOptionsSnowflake = value.extract()?;
                Ok(TableOptionsV0::Snowflake(snowflake))
            }
            Self::SQL_SERVER => {
                let sql_server: TableOptionsSqlServer = value.extract()?;
                Ok(TableOptionsV0::SqlServer(sql_server))
            }
            Self::CLICKHOUSE => {
                let clickhouse: TableOptionsClickhouse = value.extract()?;
                Ok(TableOptionsV0::Clickhouse(clickhouse))
            }
            Self::CASSANDRA => {
                let cassandra: TableOptionsCassandra = value.extract()?;
                Ok(TableOptionsV0::Cassandra(cassandra))
            }
            Self::EXCEL => {
                let excel: TableOptionsExcel = value.extract()?;
                Ok(TableOptionsV0::Excel(excel))
            }
            Self::SQLITE => {
                let sqlite: TableOptionsSqlite = value.extract()?;
                Ok(TableOptionsV0::Sqlite(sqlite))
            }
            Self::DEBUG => {
                let debug: TableOptionsDebug = value.extract()?;
                Ok(TableOptionsV0::Debug(debug))
            }
            _ => Err(ProtoConvError::UnknownVariant(value.name.to_string())),
        }
    }
}

impl fmt::Display for TableOptionsV0 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<TableOptionsV0> for options::table_options_v0::Options {
    type Error = ProtoConvError;
    fn try_from(value: TableOptionsV0) -> Result<Self, Self::Error> {
        Ok(match value {
            TableOptionsV0::Debug(v) => options::table_options_v0::Options::Debug(v.into()),
            TableOptionsV0::Internal(v) => {
                options::table_options_v0::Options::Internal(v.try_into()?)
            }
            TableOptionsV0::Postgres(v) => options::table_options_v0::Options::Postgres(v.into()),
            TableOptionsV0::BigQuery(v) => options::table_options_v0::Options::Bigquery(v.into()),
            TableOptionsV0::Mysql(v) => options::table_options_v0::Options::Mysql(v.into()),
            TableOptionsV0::Local(v) => options::table_options_v0::Options::Local(v.into()),
            TableOptionsV0::Gcs(v) => options::table_options_v0::Options::Gcs(v.into()),
            TableOptionsV0::S3(v) => options::table_options_v0::Options::S3(v.into()),
            TableOptionsV0::MongoDb(v) => options::table_options_v0::Options::Mongo(v.into()),
            TableOptionsV0::Snowflake(v) => options::table_options_v0::Options::Snowflake(v.into()),
            TableOptionsV0::Delta(v) => options::table_options_v0::Options::Delta(v.into()),
            TableOptionsV0::Iceberg(v) => options::table_options_v0::Options::Iceberg(v.into()),
            TableOptionsV0::Azure(v) => options::table_options_v0::Options::Azure(v.into()),
            TableOptionsV0::SqlServer(v) => options::table_options_v0::Options::SqlServer(v.into()),
            TableOptionsV0::Lance(v) => options::table_options_v0::Options::Lance(v.into()),
            TableOptionsV0::Bson(v) => options::table_options_v0::Options::Bson(v.into()),
            TableOptionsV0::Clickhouse(v) => {
                options::table_options_v0::Options::Clickhouse(v.into())
            }
            TableOptionsV0::Cassandra(v) => options::table_options_v0::Options::Cassandra(v.into()),
            TableOptionsV0::Excel(v) => options::table_options_v0::Options::Excel(v.into()),
            TableOptionsV0::Sqlite(v) => options::table_options_v0::Options::Sqlite(v.into()),
        })
    }
}

impl TryFrom<options::table_options_v0::Options> for TableOptionsV0 {
    type Error = ProtoConvError;
    fn try_from(value: options::table_options_v0::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            options::table_options_v0::Options::Internal(v) => {
                TableOptionsV0::Internal(v.try_into()?)
            }
            options::table_options_v0::Options::Debug(v) => TableOptionsV0::Debug(v.try_into()?),
            options::table_options_v0::Options::Postgres(v) => {
                TableOptionsV0::Postgres(v.try_into()?)
            }
            options::table_options_v0::Options::Bigquery(v) => {
                TableOptionsV0::BigQuery(v.try_into()?)
            }
            options::table_options_v0::Options::Mysql(v) => TableOptionsV0::Mysql(v.try_into()?),
            options::table_options_v0::Options::Local(v) => TableOptionsV0::Local(v.try_into()?),
            options::table_options_v0::Options::Gcs(v) => TableOptionsV0::Gcs(v.try_into()?),
            options::table_options_v0::Options::S3(v) => TableOptionsV0::S3(v.try_into()?),
            options::table_options_v0::Options::Mongo(v) => TableOptionsV0::MongoDb(v.try_into()?),
            options::table_options_v0::Options::Snowflake(v) => {
                TableOptionsV0::Snowflake(v.try_into()?)
            }
            options::table_options_v0::Options::Delta(v) => TableOptionsV0::Delta(v.try_into()?),
            options::table_options_v0::Options::Iceberg(v) => {
                TableOptionsV0::Iceberg(v.try_into()?)
            }
            options::table_options_v0::Options::Azure(v) => TableOptionsV0::Azure(v.try_into()?),
            options::table_options_v0::Options::SqlServer(v) => {
                TableOptionsV0::SqlServer(v.try_into()?)
            }
            options::table_options_v0::Options::Lance(v) => TableOptionsV0::Lance(v.try_into()?),
            options::table_options_v0::Options::Bson(v) => TableOptionsV0::Bson(v.try_into()?),
            options::table_options_v0::Options::Clickhouse(v) => {
                TableOptionsV0::Clickhouse(v.try_into()?)
            }
            options::table_options_v0::Options::Cassandra(v) => {
                TableOptionsV0::Cassandra(v.try_into()?)
            }
            options::table_options_v0::Options::Excel(v) => TableOptionsV0::Excel(v.try_into()?),
            options::table_options_v0::Options::Sqlite(v) => TableOptionsV0::Sqlite(v.try_into()?),
        })
    }
}

impl TryFrom<options::TableOptionsV0> for TableOptionsV0 {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsV0) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl TryFrom<TableOptionsV0> for options::TableOptionsV0 {
    type Error = ProtoConvError;
    fn try_from(value: TableOptionsV0) -> Result<Self, Self::Error> {
        Ok(options::TableOptionsV0 {
            options: Some(value.try_into()?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsDebug {
    pub table_type: String,
}

impl From<TableOptionsDebug> for TableOptionsV0 {
    fn from(value: TableOptionsDebug) -> Self {
        TableOptionsV0::Debug(value)
    }
}

impl Default for TableOptionsDebug {
    fn default() -> Self {
        TableOptionsDebug {
            table_type: "never_ending".to_string(),
        }
    }
}

impl TableOptionsImpl for TableOptionsDebug {
    const NAME: &'static str = "debug";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsInternal {
    pub columns: Vec<InternalColumnDefinition>,
}

impl From<TableOptionsInternal> for TableOptionsV0 {
    fn from(value: TableOptionsInternal) -> Self {
        TableOptionsV0::Internal(value)
    }
}

impl TableOptionsImpl for TableOptionsInternal {
    const NAME: &'static str = "internal";
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
            columns: value.columns.into_iter().map(Into::into).collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsPostgres {
    pub connection_string: String,
    pub schema: String,
    pub table: String,
}

impl From<TableOptionsPostgres> for TableOptionsV0 {
    fn from(value: TableOptionsPostgres) -> Self {
        TableOptionsV0::Postgres(value)
    }
}

impl TableOptionsImpl for TableOptionsPostgres {
    const NAME: &'static str = "postgres";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsBigQuery {
    pub service_account_key: String,
    pub project_id: String,
    pub dataset_id: String,
    pub table_id: String,
}

impl From<TableOptionsBigQuery> for TableOptionsV0 {
    fn from(value: TableOptionsBigQuery) -> Self {
        TableOptionsV0::BigQuery(value)
    }
}

impl TableOptionsImpl for TableOptionsBigQuery {
    const NAME: &'static str = "bigquery";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsMysql {
    pub connection_string: String,
    pub schema: String,
    pub table: String,
}

impl From<TableOptionsMysql> for TableOptionsV0 {
    fn from(value: TableOptionsMysql) -> Self {
        TableOptionsV0::Mysql(value)
    }
}

impl TableOptionsImpl for TableOptionsMysql {
    const NAME: &'static str = "mysql";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsLocal {
    pub location: String,
    pub file_type: String,
    pub compression: Option<String>,
}

impl From<TableOptionsLocal> for TableOptionsV0 {
    fn from(value: TableOptionsLocal) -> Self {
        TableOptionsV0::Local(value)
    }
}

impl TableOptionsImpl for TableOptionsLocal {
    const NAME: &'static str = "local";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsGcs {
    pub service_account_key: Option<String>,
    pub bucket: String,
    pub location: String,
    pub file_type: String,
    pub compression: Option<String>,
}

impl From<TableOptionsGcs> for TableOptionsV0 {
    fn from(value: TableOptionsGcs) -> Self {
        TableOptionsV0::Gcs(value)
    }
}

impl TableOptionsImpl for TableOptionsGcs {
    const NAME: &'static str = "gcs";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsS3 {
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub region: String,
    pub bucket: String,
    pub location: String,
    pub file_type: String,
    pub compression: Option<String>,
}

impl From<TableOptionsS3> for TableOptionsV0 {
    fn from(value: TableOptionsS3) -> Self {
        TableOptionsV0::S3(value)
    }
}

impl TableOptionsImpl for TableOptionsS3 {
    const NAME: &'static str = "s3";
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsMongoDb {
    pub connection_string: String,
    pub database: String,
    pub collection: String,
}

impl From<TableOptionsMongoDb> for TableOptionsV0 {
    fn from(value: TableOptionsMongoDb) -> Self {
        TableOptionsV0::MongoDb(value)
    }
}

impl TableOptionsImpl for TableOptionsMongoDb {
    const NAME: &'static str = "mongo";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsExcel {
    pub location: String,
    pub storage_options: StorageOptions,
    pub file_type: Option<String>,
    pub compression: Option<String>,
    pub sheet_name: Option<String>,
    pub has_header: bool,
}

impl From<TableOptionsExcel> for TableOptionsV0 {
    fn from(value: TableOptionsExcel) -> Self {
        TableOptionsV0::Excel(value)
    }
}

impl TableOptionsImpl for TableOptionsExcel {
    const NAME: &'static str = "excel";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsSqlServer {
    pub connection_string: String,
    pub schema: String,
    pub table: String,
}

impl From<TableOptionsSqlServer> for TableOptionsV0 {
    fn from(value: TableOptionsSqlServer) -> Self {
        TableOptionsV0::SqlServer(value)
    }
}

impl TableOptionsImpl for TableOptionsSqlServer {
    const NAME: &'static str = "sql_server";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsClickhouse {
    pub connection_string: String,
    pub table: String,
    pub database: Option<String>,
}

impl From<TableOptionsClickhouse> for TableOptionsV0 {
    fn from(value: TableOptionsClickhouse) -> Self {
        TableOptionsV0::Clickhouse(value)
    }
}

impl TableOptionsImpl for TableOptionsClickhouse {
    const NAME: &'static str = "clickhouse";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsCassandra {
    pub host: String,
    pub keyspace: String,
    pub table: String,
    pub username: Option<String>,
    pub password: Option<String>,
}

impl From<TableOptionsCassandra> for TableOptionsV0 {
    fn from(value: TableOptionsCassandra) -> Self {
        TableOptionsV0::Cassandra(value)
    }
}

impl TableOptionsImpl for TableOptionsCassandra {
    const NAME: &'static str = "cassandra";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsSqlite {
    pub location: String,
    pub table: String,
}

impl From<TableOptionsSqlite> for TableOptionsV0 {
    fn from(value: TableOptionsSqlite) -> Self {
        TableOptionsV0::Sqlite(value)
    }
}

impl TableOptionsImpl for TableOptionsSqlite {
    const NAME: &'static str = "sqlite";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

impl From<TableOptionsSnowflake> for TableOptionsV0 {
    fn from(value: TableOptionsSnowflake) -> Self {
        TableOptionsV0::Snowflake(value)
    }
}

impl TableOptionsImpl for TableOptionsSnowflake {
    const NAME: &'static str = "snowflake";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsObjectStore {
    pub location: String,
    pub storage_options: StorageOptions,
    pub file_type: Option<String>,
    pub compression: Option<String>,
    pub schema_sample_size: Option<i64>,
}

impl TableOptionsImpl for TableOptionsObjectStore {
    const NAME: &'static str = "object_store";
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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
