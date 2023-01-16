#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CatalogState {
    /// Database that this catalog is for.
    ///
    /// The bytes should be convertible into a UUID (V4).
    #[prost(bytes = "vec", tag = "1")]
    pub db_id: ::prost::alloc::vec::Vec<u8>,
    /// Version of this catalog. Increments on every mutation.
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
    #[prost(oneof = "catalog_entry::Entry", tags = "1, 2, 3, 4, 5")]
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
        /// External database tables.
        ExternalTable = 3,
        /// Database views.
        View = 4,
        /// Connections to external data sources.
        Connection = 5,
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
                EntryType::ExternalTable => "EXTERNAL_TABLE",
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
                "EXTERNAL_TABLE" => Some(Self::ExternalTable),
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
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TableOptions {
    #[prost(oneof = "table_options::Options", tags = "1, 2")]
    pub options: ::core::option::Option<table_options::Options>,
}
/// Nested message and enum types in `TableOptions`.
pub mod table_options {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Options {
        #[prost(message, tag = "1")]
        Debug(super::TableOptionsDebug),
        #[prost(message, tag = "2")]
        Postgres(super::TableOptionsPostgres),
    }
}
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
    /// Options related to this connection.
    ///
    /// The connection type is derived from these options.
    #[prost(message, optional, tag = "2")]
    pub options: ::core::option::Option<ConnectionOptions>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ConnectionOptions {
    #[prost(oneof = "connection_options::Options", tags = "1, 2")]
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
}
