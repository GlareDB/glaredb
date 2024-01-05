use crate::gen::common::arrow;
use crate::gen::metastore::options;
use crate::{FromOptionalField, ProtoConvError};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::{
    arrow::datatypes::{DataType, Field},
    common::DFSchemaRef,
};
use proptest_derive::Arbitrary;
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub struct InternalColumnDefinition {
    pub name: String,
    pub nullable: bool,
    // TODO: change proptest strategy to select random DataType
    #[proptest(value("DataType::Utf8"))]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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
#[derive(Debug, Default, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub enum TableOptions {
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
}

impl TableOptions {
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

    pub const fn new_internal(columns: Vec<InternalColumnDefinition>) -> TableOptions {
        TableOptions::Internal(TableOptionsInternal { columns })
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            TableOptions::Internal(_) => Self::INTERNAL,
            TableOptions::Debug(_) => Self::DEBUG,
            TableOptions::Postgres(_) => Self::POSTGRES,
            TableOptions::BigQuery(_) => Self::BIGQUERY,
            TableOptions::Mysql(_) => Self::MYSQL,
            TableOptions::Local(_) => Self::LOCAL,
            TableOptions::Gcs(_) => Self::GCS,
            TableOptions::S3(_) => Self::S3_STORAGE,
            TableOptions::MongoDb(_) => Self::MONGODB,
            TableOptions::Snowflake(_) => Self::SNOWFLAKE,
            TableOptions::Delta(_) => Self::DELTA,
            TableOptions::Iceberg(_) => Self::ICEBERG,
            TableOptions::Azure(_) => Self::AZURE,
            TableOptions::SqlServer(_) => Self::SQL_SERVER,
            TableOptions::Lance(_) => Self::LANCE,
            TableOptions::Bson(_) => Self::BSON,
            TableOptions::Clickhouse(_) => Self::CLICKHOUSE,
            TableOptions::Cassandra(_) => Self::CASSANDRA,
        }
    }
}

impl fmt::Display for TableOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TryFrom<options::table_options::Options> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::table_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            options::table_options::Options::Internal(v) => TableOptions::Internal(v.try_into()?),
            options::table_options::Options::Debug(v) => TableOptions::Debug(v.try_into()?),
            options::table_options::Options::Postgres(v) => TableOptions::Postgres(v.try_into()?),
            options::table_options::Options::Bigquery(v) => TableOptions::BigQuery(v.try_into()?),
            options::table_options::Options::Mysql(v) => TableOptions::Mysql(v.try_into()?),
            options::table_options::Options::Local(v) => TableOptions::Local(v.try_into()?),
            options::table_options::Options::Gcs(v) => TableOptions::Gcs(v.try_into()?),
            options::table_options::Options::S3(v) => TableOptions::S3(v.try_into()?),
            options::table_options::Options::Mongo(v) => TableOptions::MongoDb(v.try_into()?),
            options::table_options::Options::Snowflake(v) => TableOptions::Snowflake(v.try_into()?),
            options::table_options::Options::Delta(v) => TableOptions::Delta(v.try_into()?),
            options::table_options::Options::Iceberg(v) => TableOptions::Iceberg(v.try_into()?),
            options::table_options::Options::Azure(v) => TableOptions::Azure(v.try_into()?),
            options::table_options::Options::SqlServer(v) => TableOptions::SqlServer(v.try_into()?),
            options::table_options::Options::Lance(v) => TableOptions::Lance(v.try_into()?),
            options::table_options::Options::Bson(v) => TableOptions::Bson(v.try_into()?),
            options::table_options::Options::Clickhouse(v) => {
                TableOptions::Clickhouse(v.try_into()?)
            }
            options::table_options::Options::Cassandra(v) => TableOptions::Cassandra(v.try_into()?),
        })
    }
}

impl TryFrom<options::TableOptions> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl TryFrom<TableOptions> for options::table_options::Options {
    type Error = ProtoConvError;
    fn try_from(value: TableOptions) -> Result<Self, Self::Error> {
        Ok(match value {
            TableOptions::Internal(v) => options::table_options::Options::Internal(v.try_into()?),
            TableOptions::Debug(v) => options::table_options::Options::Debug(v.into()),
            TableOptions::Postgres(v) => options::table_options::Options::Postgres(v.into()),
            TableOptions::BigQuery(v) => options::table_options::Options::Bigquery(v.into()),
            TableOptions::Mysql(v) => options::table_options::Options::Mysql(v.into()),
            TableOptions::Local(v) => options::table_options::Options::Local(v.into()),
            TableOptions::Gcs(v) => options::table_options::Options::Gcs(v.into()),
            TableOptions::S3(v) => options::table_options::Options::S3(v.into()),
            TableOptions::MongoDb(v) => options::table_options::Options::Mongo(v.into()),
            TableOptions::Snowflake(v) => options::table_options::Options::Snowflake(v.into()),
            TableOptions::Delta(v) => options::table_options::Options::Delta(v.into()),
            TableOptions::Iceberg(v) => options::table_options::Options::Iceberg(v.into()),
            TableOptions::Azure(v) => options::table_options::Options::Azure(v.into()),
            TableOptions::SqlServer(v) => options::table_options::Options::SqlServer(v.into()),
            TableOptions::Lance(v) => options::table_options::Options::Lance(v.into()),
            TableOptions::Bson(v) => options::table_options::Options::Bson(v.into()),
            TableOptions::Clickhouse(v) => options::table_options::Options::Clickhouse(v.into()),
            TableOptions::Cassandra(v) => options::table_options::Options::Cassandra(v.into()),
        })
    }
}

impl TryFrom<TableOptions> for options::TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: TableOptions) -> Result<Self, Self::Error> {
        Ok(options::TableOptions {
            options: Some(value.try_into()?),
        })
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub struct TableOptionsInternal {
    pub columns: Vec<InternalColumnDefinition>,
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub struct TableOptionsDebug {
    pub table_type: String,
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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
#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub struct TableOptionsClickhouse {
    pub connection_string: String,
    pub table: String,
}

impl TryFrom<options::TableOptionsClickhouse> for TableOptionsClickhouse {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsClickhouse) -> Result<Self, Self::Error> {
        Ok(TableOptionsClickhouse {
            connection_string: value.connection_string,
            table: value.table,
        })
    }
}

impl From<TableOptionsClickhouse> for options::TableOptionsClickhouse {
    fn from(value: TableOptionsClickhouse) -> Self {
        options::TableOptionsClickhouse {
            connection_string: value.connection_string,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub struct TableOptionsCassandra {
    pub host: String,
    pub keyspace: String,
    pub table: String,
}

impl TryFrom<options::TableOptionsCassandra> for TableOptionsCassandra {
    type Error = ProtoConvError;
    fn try_from(value: options::TableOptionsCassandra) -> Result<Self, Self::Error> {
        Ok(TableOptionsCassandra {
            host: value.host,
            keyspace: value.keyspace,
            table: value.table,
        })
    }
}

impl From<TableOptionsCassandra> for options::TableOptionsCassandra {
    fn from(value: TableOptionsCassandra) -> Self {
        options::TableOptionsCassandra {
            host: value.host,
            keyspace: value.keyspace,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::arbitrary::any;
    use proptest::proptest;

    proptest! {
        #[test]
        fn roundtrip_table_options(expected in any::<TableOptions>()) {
            let p: options::TableOptions = expected.clone().try_into().unwrap();
            let got: TableOptions = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_connection_options(expected in any::<DatabaseOptions>()) {
            let p: options::DatabaseOptions = expected.clone().into();
            let got: DatabaseOptions = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
pub enum CredentialsOptions {
    Debug(CredentialsOptionsDebug),
    Gcp(CredentialsOptionsGcp),
    Aws(CredentialsOptionsAws),
    Azure(CredentialsOptionsAzure),
}

impl CredentialsOptions {
    pub const DEBUG: &'static str = "debug";
    pub const GCP: &'static str = "gcp";
    pub const AWS: &'static str = "aws";
    pub const AZURE: &'static str = "azure";

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Debug(_) => Self::DEBUG,
            Self::Gcp(_) => Self::GCP,
            Self::Aws(_) => Self::AWS,
            Self::Azure(_) => Self::AZURE,
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq, Hash)]
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
    pub location: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum CopyToFormatOptions {
    Csv(CopyToFormatOptionsCsv),
    Parquet(CopyToFormatOptionsParquet),
    Json(CopyToFormatOptionsJson),
    Bson,
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

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Csv(_) => Self::CSV,
            Self::Parquet(_) => Self::PARQUET,
            Self::Json(_) => Self::JSON,
            Self::Bson => Self::BSON,
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
