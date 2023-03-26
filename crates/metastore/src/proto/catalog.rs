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
    #[prost(oneof = "catalog_entry::Entry", tags = "1, 2, 3, 4")]
    pub entry: ::core::option::Option<catalog_entry::Entry>,
}
/// Nested message and enum types in `CatalogEntry`.
pub mod catalog_entry {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Entry {
        #[prost(message, tag = "1")]
        Database(super::DatabaseEntry),
        #[prost(message, tag = "2")]
        Schema(super::SchemaEntry),
        #[prost(message, tag = "3")]
        Table(super::TableEntry),
        #[prost(message, tag = "4")]
        View(super::ViewEntry),
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
    ///
    /// Database entries will have a parent id of 0.
    #[prost(uint32, tag = "3")]
    pub parent: u32,
    /// Name of this entry.
    #[prost(string, tag = "4")]
    pub name: ::prost::alloc::string::String,
    /// Whether or not this entry is builtin. Builtin entries cannot be dropped.
    #[prost(bool, tag = "5")]
    pub builtin: bool,
    /// If this entry is backed by an external system or resource (e.g. external
    /// database or external table).
    #[prost(bool, tag = "6")]
    pub external: bool,
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
        /// Internal or external database.
        Database = 1,
        /// Internal schema (eventually include external)
        Schema = 2,
        /// Internal or external table.
        Table = 3,
        /// Internal view.
        View = 4,
    }
    impl EntryType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EntryType::Unknown => "UNKNOWN",
                EntryType::Database => "DATABASE",
                EntryType::Schema => "SCHEMA",
                EntryType::Table => "TABLE",
                EntryType::View => "VIEW",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "DATABASE" => Some(Self::Database),
                "SCHEMA" => Some(Self::Schema),
                "TABLE" => Some(Self::Table),
                "VIEW" => Some(Self::View),
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
    pub options: ::core::option::Option<super::options::DatabaseOptions>,
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
    #[prost(message, optional, tag = "3")]
    pub options: ::core::option::Option<super::options::TableOptions>,
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
