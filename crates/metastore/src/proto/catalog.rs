#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogState {
    /// Database that this catalog is for (UUID).
    #[prost(string, tag = "1")]
    pub db_id: ::prost::alloc::string::String,
    /// Version of this catalog.
    #[prost(uint64, tag = "2")]
    pub version: u64,
    /// All entries in this catalog.
    ///
    /// ID -> Entry
    #[prost(map = "uint32, message", tag = "3")]
    pub entries: ::std::collections::HashMap<u32, CatalogEntry>,
}
/// Possible top-level catalog entries.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogEntry {
    #[prost(oneof = "catalog_entry::Entry", tags = "1, 2, 3, 4")]
    pub entry: ::core::option::Option<catalog_entry::Entry>,
}
/// Nested message and enum types in `CatalogEntry`.
pub mod catalog_entry {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Entry {
        #[prost(message, tag = "1")]
        Schema(super::SchemaEntry),
        #[prost(message, tag = "2")]
        Table(super::TableEntry),
        #[prost(message, tag = "3")]
        View(super::ViewEntry),
        #[prost(message, tag = "4")]
        Connection(super::ConnectionEntry),
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
    /// For tables, views, and connections, the parent id will be the schema id.
    ///
    /// Schemas are a special case, and have a parent id of 0.
    #[prost(uint32, tag = "3")]
    pub parent: u32,
    /// Name of this entry.
    #[prost(string, tag = "4")]
    pub name: ::prost::alloc::string::String,
}
/// Nested message and enum types in `EntryMeta`.
pub mod entry_meta {
    /// Possible entry types in the catalog.
    ///
    /// Each entry of this type shares the same ID space.
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
        View = 3,
        /// Connections to external data sources.
        Connection = 4,
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
                _ => None,
            }
        }
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SchemaEntry {
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
pub struct ColumnDefinition {
    /// Name of the column in the table.
    #[prost(string, tag = "1")]
    pub name: ::prost::alloc::string::String,
    /// Ordinal of the column in the table.
    #[prost(uint32, tag = "2")]
    pub ord: u32,
    /// Field is nullable.
    #[prost(bool, tag = "3")]
    pub nullable: bool,
    /// Arrow type for the field.
    ///
    /// Note this will likely need to be expanded for complex types.
    #[prost(message, optional, tag = "4")]
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
    #[prost(enumeration = "ConnectionType", tag = "2")]
    pub connection_type: i32,
    /// Details about this connection.
    #[prost(message, optional, tag = "3")]
    pub details: ::core::option::Option<ConnectionDetails>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionDetails {
    #[prost(oneof = "connection_details::Connection", tags = "1, 2")]
    pub connection: ::core::option::Option<connection_details::Connection>,
}
/// Nested message and enum types in `ConnectionDetails`.
pub mod connection_details {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Connection {
        #[prost(message, tag = "1")]
        Debug(super::ConnectionDebug),
        #[prost(message, tag = "2")]
        Postgres(super::ConnectionPostgres),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionDebug {}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionPostgres {
    #[prost(string, tag = "1")]
    pub connection_string: ::prost::alloc::string::String,
}
/// Supported connection types.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ConnectionType {
    Unknown = 0,
    Debug = 1,
    Postgres = 2,
    Bigquery = 3,
    Gcs = 4,
    S3 = 5,
    Local = 6,
}
impl ConnectionType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ConnectionType::Unknown => "CONNECTION_TYPE_UNKNOWN",
            ConnectionType::Debug => "CONNECTION_TYPE_DEBUG",
            ConnectionType::Postgres => "CONNECTION_TYPE_POSTGRES",
            ConnectionType::Bigquery => "CONNECTION_TYPE_BIGQUERY",
            ConnectionType::Gcs => "CONNECTION_TYPE_GCS",
            ConnectionType::S3 => "CONNECTION_TYPE_S3",
            ConnectionType::Local => "CONNECTION_TYPE_LOCAL",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "CONNECTION_TYPE_UNKNOWN" => Some(Self::Unknown),
            "CONNECTION_TYPE_DEBUG" => Some(Self::Debug),
            "CONNECTION_TYPE_POSTGRES" => Some(Self::Postgres),
            "CONNECTION_TYPE_BIGQUERY" => Some(Self::Bigquery),
            "CONNECTION_TYPE_GCS" => Some(Self::Gcs),
            "CONNECTION_TYPE_S3" => Some(Self::S3),
            "CONNECTION_TYPE_LOCAL" => Some(Self::Local),
            _ => None,
        }
    }
}
