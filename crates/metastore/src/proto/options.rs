#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptions {
    #[prost(oneof = "database_options::Options", tags = "1, 2, 3, 4, 5, 6, 7")]
    pub options: ::core::option::Option<database_options::Options>,
}
/// Nested message and enum types in `DatabaseOptions`.
pub mod database_options {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Options {
        #[prost(message, tag = "1")]
        Internal(super::DatabaseOptionsInternal),
        #[prost(message, tag = "2")]
        Debug(super::DatabaseOptionsDebug),
        #[prost(message, tag = "3")]
        Postgres(super::DatabaseOptionsPostgres),
        #[prost(message, tag = "4")]
        Bigquery(super::DatabaseOptionsBigQuery),
        #[prost(message, tag = "5")]
        Mysql(super::DatabaseOptionsMysql),
        #[prost(message, tag = "6")]
        Mongo(super::DatabaseOptionsMongo),
        #[prost(message, tag = "7")]
        Snowflake(super::DatabaseOptionsSnowflake),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsInternal {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsDebug {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsPostgres {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsBigQuery {
    #[prost(string, tag = "1")]
    pub service_account_key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub project_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsMysql {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsMongo {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsSnowflake {
    #[prost(string, tag = "1")]
    pub account_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub login_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub database_name: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub warehouse: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub role_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptions {
    #[prost(oneof = "table_options::Options", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10")]
    pub options: ::core::option::Option<table_options::Options>,
}
/// Nested message and enum types in `TableOptions`.
pub mod table_options {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Options {
        #[prost(message, tag = "1")]
        Internal(super::TableOptionsInternal),
        #[prost(message, tag = "2")]
        Debug(super::TableOptionsDebug),
        #[prost(message, tag = "3")]
        Postgres(super::TableOptionsPostgres),
        #[prost(message, tag = "4")]
        Bigquery(super::TableOptionsBigQuery),
        #[prost(message, tag = "5")]
        Local(super::TableOptionsLocal),
        #[prost(message, tag = "6")]
        Gcs(super::TableOptionsGcs),
        #[prost(message, tag = "7")]
        S3(super::TableOptionsS3),
        #[prost(message, tag = "8")]
        Mysql(super::TableOptionsMysql),
        #[prost(message, tag = "9")]
        Mongo(super::TableOptionsMongo),
        #[prost(message, tag = "10")]
        Snowflake(super::TableOptionsSnowflake),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsInternal {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsDebug {
    #[prost(string, tag = "1")]
    pub table_type: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsPostgres {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsBigQuery {
    #[prost(string, tag = "1")]
    pub service_account_key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub project_id: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub dataset_id: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub table_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsMysql {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub schema: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsLocal {
    #[prost(string, tag = "1")]
    pub location: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsGcs {
    #[prost(string, tag = "1")]
    pub service_account_key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub bucket: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub location: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsS3 {
    #[prost(string, tag = "1")]
    pub access_key_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub secret_access_key: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub region: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub bucket: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub location: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsMongo {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub database: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub collection: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsSnowflake {
    #[prost(string, tag = "1")]
    pub account_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub login_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub password: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub database_name: ::prost::alloc::string::String,
    #[prost(string, tag = "5")]
    pub warehouse: ::prost::alloc::string::String,
    #[prost(string, tag = "6")]
    pub role_name: ::prost::alloc::string::String,
    #[prost(string, tag = "7")]
    pub schema_name: ::prost::alloc::string::String,
    #[prost(string, tag = "8")]
    pub table_name: ::prost::alloc::string::String,
}
