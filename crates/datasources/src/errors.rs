#[derive(Debug, thiserror::Error)]
pub enum DataSourceError {
    #[error(transparent)]
    BigQuery(#[from] crate::bigquery::errors::BigQueryError),

    #[error(transparent)]
    Debug(#[from] crate::debug::errors::DebugError),

    #[error(transparent)]
    MongoDb(#[from] crate::mongodb::errors::MongoError),

    #[error(transparent)]
    Mysql(#[from] crate::mysql::errors::MysqlError),

    #[error(transparent)]
    ObjectStore(#[from] crate::object_store::errors::ObjectStoreSourceError),

    #[error(transparent)]
    Postgres(#[from] crate::postgres::errors::PostgresError),

    #[error(transparent)]
    Snowflake(#[from] crate::snowflake::errors::DatasourceSnowflakeError),
}
