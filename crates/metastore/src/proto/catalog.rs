/// The state of the catalog at some version.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogState {
    /// Version of this catalog. Increments on every mutation.
    #[prost(uint64, tag = "1")]
    pub version: u64,
    /// All entries in this catalog.
    ///
    /// ID -> Entry
    #[prost(map = "uint32, message", tag = "2")]
    pub entries: ::std::collections::HashMap<u32, CatalogEntry>,
}
/// Possible top-level catalog entries.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogEntry {
    #[prost(oneof = "catalog_entry::Entry", tags = "6, 1, 2, 3, 4, 5")]
    pub entry: ::core::option::Option<catalog_entry::Entry>,
}
/// Nested message and enum types in `CatalogEntry`.
pub mod catalog_entry {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Entry {
        /// TODO
        #[prost(message, tag = "6")]
        Database(super::DatabaseEntry),
        #[prost(message, tag = "1")]
        Schema(super::SchemaEntry),
        #[prost(message, tag = "2")]
        Table(super::TableEntry),
        #[prost(message, tag = "3")]
        View(super::ViewEntry),
        #[prost(message, tag = "4")]
        Connection(super::ConnectionEntry),
        #[prost(message, tag = "5")]
        ExternalTable(super::ExternalTableEntry),
    }
}
/// Metadata for every entry in the catalog.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EntryMeta {
    /// Type of the entry.
    #[prost(enumeration = "entry_meta::EntryType", tag = "1")]
    pub entry_type: i32,
    /// ID of the entry. This id must be unique within the database, and will act
    /// similarly to Postgres' OIDs.
    ///
    /// System entries have well-known IDs.
    #[prost(uint32, tag = "2")]
    pub id: u32,
    /// ID of the parent entry.
    ///
    /// For tables and views, the parent id will be the schema id.
    ///
    /// For schemas, the parent will be the database id.
    #[prost(uint32, tag = "3")]
    pub parent: u32,
    /// Name of this entry.
    #[prost(string, tag = "4")]
    pub name: ::prost::alloc::string::String,
    /// Whether or not this entry is builtin. Builtin entries cannot be dropped.
    #[prost(bool, tag = "5")]
    pub builtin: bool,
}
/// Nested message and enum types in `EntryMeta`.
pub mod entry_meta {
    /// Possible entry types in the catalog.
    ///
    /// Each entry of this type shares the same ID space.
    /// TODO: Renumber
    #[derive(
        Clone,
        Copy,
        Debug,
        PartialEq,
        Eq,
        Hash,
        PartialOrd,
        Ord,
        ::prost::Enumeration
    )]
    #[repr(i32)]
    pub enum EntryType {
        /// Unknown catalog entry. We should error if this is encountered.
        Unknown = 0,
        /// Database schemas.
        Schema = 1,
        /// Database tables.
        Table = 2,
        /// Database views.
        View = 4,
        /// Connections to external data sources.
        Connection = 5,
        ExternalTable = 3,
        Database = 6,
    }
    impl EntryType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EntryType::Unknown => "UNKNOWN",
                EntryType::Schema => "SCHEMA",
                EntryType::Table => "TABLE",
                EntryType::View => "VIEW",
                EntryType::Connection => "CONNECTION",
                EntryType::ExternalTable => "EXTERNAL_TABLE",
                EntryType::Database => "DATABASE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "SCHEMA" => Some(Self::Schema),
                "TABLE" => Some(Self::Table),
                "VIEW" => Some(Self::View),
                "CONNECTION" => Some(Self::Connection),
                "EXTERNAL_TABLE" => Some(Self::ExternalTable),
                "DATABASE" => Some(Self::Database),
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseEntry {
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<EntryMeta>,
    /// next: 3
    #[prost(message, optional, tag = "2")]
    pub options: ::core::option::Option<DatabaseOptions>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptions {
    /// TODO
    #[prost(oneof = "database_options::Options", tags = "1, 2, 3")]
    pub options: ::core::option::Option<database_options::Options>,
}
/// Nested message and enum types in `DatabaseOptions`.
pub mod database_options {
    /// TODO
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Options {
        #[prost(message, tag = "1")]
        Internal(super::DatabaseOptionsInternal),
        #[prost(message, tag = "2")]
        Postgres(super::DatabaseOptionsPostgres),
        #[prost(message, tag = "3")]
        Bigquery(super::DatabaseOptionsBigQuery),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DatabaseOptionsInternal {}
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
pub struct SchemaEntry {
    /// next: 2
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<EntryMeta>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableEntry {
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<EntryMeta>,
    /// Columns in the table.
    #[prost(message, repeated, tag = "2")]
    pub columns: ::prost::alloc::vec::Vec<ColumnDefinition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ExternalTableEntry {
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<EntryMeta>,
    /// ID to the connection to use.
    #[prost(uint32, tag = "2")]
    pub connection_id: u32,
    /// Table specific options to use when connecting to the external table.
    ///
    /// The external table type (postgres, bigquery, etc) is derived from these
    /// options. The type derived here must match the connection type.
    #[prost(message, optional, tag = "3")]
    pub options: ::core::option::Option<TableOptions>,
    /// Columns in the external table.
    #[prost(message, repeated, tag = "4")]
    pub columns: ::prost::alloc::vec::Vec<ColumnDefinition>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptions {
    /// TODO: Renumber
    #[prost(oneof = "table_options::Options", tags = "10, 1, 2, 3, 4, 5, 6, 7, 9")]
    pub options: ::core::option::Option<table_options::Options>,
}
/// Nested message and enum types in `TableOptions`.
pub mod table_options {
    /// TODO: Renumber
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Options {
        #[prost(message, tag = "10")]
        Internal(super::TableOptionsInternal),
        #[prost(message, tag = "1")]
        Debug(super::TableOptionsDebug),
        #[prost(message, tag = "2")]
        Postgres(super::TableOptionsPostgres),
        #[prost(message, tag = "3")]
        Bigquery(super::TableOptionsBigQuery),
        #[prost(message, tag = "4")]
        Local(super::TableOptionsLocal),
        #[prost(message, tag = "5")]
        Gcs(super::TableOptionsGcs),
        #[prost(message, tag = "6")]
        S3(super::TableOptionsS3),
        #[prost(message, tag = "7")]
        Mysql(super::TableOptionsMysql),
        #[prost(message, tag = "9")]
        Mongo(super::TableOptionsMongo),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsInternal {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsDebug {
    /// TODO: Probably make thise well-known id.
    #[prost(string, tag = "1")]
    pub table_type: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsPostgres {
    /// Source schema to connect to on Postgres.
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    /// Source table to connect to.
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsBigQuery {
    /// The dataset where table belongs.
    #[prost(string, tag = "1")]
    pub dataset_id: ::prost::alloc::string::String,
    /// Name of the table.
    #[prost(string, tag = "2")]
    pub table_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsLocal {
    /// File path on the local machine.
    #[prost(string, tag = "1")]
    pub location: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsGcs {
    /// Bucket the file belongs to.
    #[prost(string, tag = "1")]
    pub bucket_name: ::prost::alloc::string::String,
    /// Name of the object.
    #[prost(string, tag = "2")]
    pub location: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsS3 {
    /// Region the bucket belongs to.
    #[prost(string, tag = "1")]
    pub region: ::prost::alloc::string::String,
    /// Bucket the file belongs to.
    #[prost(string, tag = "2")]
    pub bucket_name: ::prost::alloc::string::String,
    /// Name of the object.
    #[prost(string, tag = "3")]
    pub location: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsMysql {
    /// Source schema to connect to on Mysql.
    #[prost(string, tag = "1")]
    pub schema: ::prost::alloc::string::String,
    /// Source table to connect to.
    #[prost(string, tag = "2")]
    pub table: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptionsMongo {
    /// Database containing the collection.
    #[prost(string, tag = "1")]
    pub database: ::prost::alloc::string::String,
    /// The collection (table).
    #[prost(string, tag = "2")]
    pub collection: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ColumnDefinition {
    /// Name of the column in the table.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Field is nullable.
    #[prost(bool, tag = "2")]
    pub nullable: bool,
    /// Arrow type for the field.
    ///
    /// Note this will likely need to be expanded for complex types.
    #[prost(message, optional, tag = "3")]
    pub arrow_type: ::core::option::Option<super::arrow::ArrowType>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ViewEntry {
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<EntryMeta>,
    /// The sql statement for materializing the view.
    #[prost(string, tag = "2")]
    pub sql: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionEntry {
    #[prost(message, optional, tag = "1")]
    pub meta: ::core::option::Option<EntryMeta>,
    /// Options related to this connection.
    ///
    /// The connection type is derived from these options.
    #[prost(message, optional, tag = "2")]
    pub options: ::core::option::Option<ConnectionOptions>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptions {
    #[prost(oneof = "connection_options::Options", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9")]
    pub options: ::core::option::Option<connection_options::Options>,
}
/// Nested message and enum types in `ConnectionOptions`.
pub mod connection_options {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Options {
        #[prost(message, tag = "1")]
        Debug(super::ConnectionOptionsDebug),
        #[prost(message, tag = "2")]
        Postgres(super::ConnectionOptionsPostgres),
        #[prost(message, tag = "3")]
        Bigquery(super::ConnectionOptionsBigQuery),
        #[prost(message, tag = "4")]
        Local(super::ConnectionOptionsLocal),
        #[prost(message, tag = "5")]
        Gcs(super::ConnectionOptionsGcs),
        #[prost(message, tag = "6")]
        S3(super::ConnectionOptionsS3),
        #[prost(message, tag = "7")]
        Ssh(super::ConnectionOptionsSsh),
        #[prost(message, tag = "8")]
        Mysql(super::ConnectionOptionsMysql),
        #[prost(message, tag = "9")]
        Mongo(super::ConnectionOptionsMongo),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsDebug {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsPostgres {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
    /// Connection id for ssh tunnel
    #[prost(uint32, optional, tag = "3")]
    pub ssh_tunnel: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsBigQuery {
    #[prost(string, tag = "1")]
    pub service_account_key: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub project_id: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsMysql {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
    /// Connection id for ssh tunnel
    #[prost(uint32, optional, tag = "3")]
    pub ssh_tunnel: ::core::option::Option<u32>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsLocal {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsGcs {
    #[prost(string, tag = "1")]
    pub service_account_key: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsS3 {
    #[prost(string, tag = "1")]
    pub access_key_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub secret_access_key: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsSsh {
    #[prost(string, tag = "1")]
    pub host: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub user: ::prost::alloc::string::String,
    /// Note the value here should not be larger than uint16 MAX
    #[prost(uint32, tag = "3")]
    pub port: u32,
    #[prost(bytes = "vec", tag = "4")]
    pub keypair: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptionsMongo {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
}
