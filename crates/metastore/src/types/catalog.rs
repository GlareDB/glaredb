use super::{FromOptionalField, ProtoConvError};
use crate::proto::arrow;
use crate::proto::catalog;
use datafusion::arrow::datatypes::DataType;
use proptest_derive::Arbitrary;
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CatalogState {
    pub db_id: Uuid,
    pub version: u64,
    pub entries: HashMap<u32, CatalogEntry>,
}

impl TryFrom<catalog::CatalogState> for CatalogState {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CatalogState) -> Result<Self, Self::Error> {
        let mut entries = HashMap::with_capacity(value.entries.len());
        for (id, ent) in value.entries {
            entries.insert(id, ent.try_into()?);
        }
        Ok(CatalogState {
            db_id: Uuid::from_slice(&value.db_id)?,
            version: value.version,
            entries,
        })
    }
}

impl TryFrom<CatalogState> for catalog::CatalogState {
    type Error = ProtoConvError;
    fn try_from(value: CatalogState) -> Result<Self, Self::Error> {
        Ok(catalog::CatalogState {
            db_id: value.db_id.into_bytes().to_vec(),
            version: value.version,
            entries: value
                .entries
                .into_iter()
                .map(|(id, ent)| match ent.try_into() {
                    Ok(ent) => Ok((id, ent)),
                    Err(e) => Err(e),
                })
                .collect::<Result<_, _>>()?,
        })
    }
}

// TODO: Implement Arbitrary and add test. This would require implementing
// Arbitrary for arrow's DataType.
#[derive(Debug, Clone)]
pub enum CatalogEntry {
    Schema(SchemaEntry),
    Table(TableEntry),
    View(ViewEntry),
    Connection(ConnectionEntry),
    ExternalTable(ExternalTableEntry),
}

impl CatalogEntry {
    pub const fn entry_type(&self) -> EntryType {
        match self {
            CatalogEntry::Schema(_) => EntryType::Schema,
            CatalogEntry::View(_) => EntryType::View,
            CatalogEntry::Table(_) => EntryType::Table,
            CatalogEntry::Connection(_) => EntryType::Connection,
            CatalogEntry::ExternalTable(_) => EntryType::ExternalTable,
        }
    }

    pub const fn is_schema(&self) -> bool {
        matches!(self, CatalogEntry::Schema(_))
    }

    /// Get the entry metadata.
    pub fn get_meta(&self) -> &EntryMeta {
        match self {
            CatalogEntry::Schema(schema) => &schema.meta,
            CatalogEntry::View(view) => &view.meta,
            CatalogEntry::Table(table) => &table.meta,
            CatalogEntry::Connection(conn) => &conn.meta,
            CatalogEntry::ExternalTable(tbl) => &tbl.meta,
        }
    }
}

impl TryFrom<catalog::catalog_entry::Entry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::catalog_entry::Entry) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::catalog_entry::Entry::Schema(v) => CatalogEntry::Schema(v.try_into()?),
            catalog::catalog_entry::Entry::View(v) => CatalogEntry::View(v.try_into()?),
            catalog::catalog_entry::Entry::Connection(v) => CatalogEntry::Connection(v.try_into()?),
            catalog::catalog_entry::Entry::ExternalTable(v) => {
                CatalogEntry::ExternalTable(v.try_into()?)
            }
            catalog::catalog_entry::Entry::Table(_) => todo!(),
        })
    }
}

impl TryFrom<catalog::CatalogEntry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CatalogEntry) -> Result<Self, Self::Error> {
        value.entry.required("entry")
    }
}

impl TryFrom<CatalogEntry> for catalog::CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: CatalogEntry) -> Result<Self, Self::Error> {
        let ent = match value {
            CatalogEntry::Schema(v) => catalog::catalog_entry::Entry::Schema(v.into()),
            CatalogEntry::View(v) => catalog::catalog_entry::Entry::View(v.into()),
            CatalogEntry::Table(v) => catalog::catalog_entry::Entry::Table(v.try_into()?),
            CatalogEntry::Connection(v) => catalog::catalog_entry::Entry::Connection(v.into()),
            CatalogEntry::ExternalTable(v) => {
                catalog::catalog_entry::Entry::ExternalTable(v.into())
            }
        };
        Ok(catalog::CatalogEntry { entry: Some(ent) })
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq)]
pub enum EntryType {
    Schema,
    ExternalTable,
    Table,
    View,
    Connection,
}

impl TryFrom<i32> for EntryType {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        catalog::entry_meta::EntryType::from_i32(value)
            .ok_or(ProtoConvError::UnknownEnumVariant("EntryType", value))
            .and_then(|t| t.try_into())
    }
}

impl TryFrom<catalog::entry_meta::EntryType> for EntryType {
    type Error = ProtoConvError;
    fn try_from(value: catalog::entry_meta::EntryType) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::entry_meta::EntryType::Unknown => {
                return Err(ProtoConvError::ZeroValueEnumVariant("EntryType"))
            }
            catalog::entry_meta::EntryType::Schema => EntryType::Schema,
            catalog::entry_meta::EntryType::ExternalTable => EntryType::ExternalTable,
            catalog::entry_meta::EntryType::Table => EntryType::Table,
            catalog::entry_meta::EntryType::View => EntryType::View,
            catalog::entry_meta::EntryType::Connection => EntryType::Connection,
        })
    }
}

impl From<EntryType> for catalog::entry_meta::EntryType {
    fn from(value: EntryType) -> Self {
        match value {
            EntryType::Schema => catalog::entry_meta::EntryType::Schema,
            EntryType::Table => catalog::entry_meta::EntryType::Table,
            EntryType::ExternalTable => catalog::entry_meta::EntryType::ExternalTable,
            EntryType::View => catalog::entry_meta::EntryType::View,
            EntryType::Connection => catalog::entry_meta::EntryType::Connection,
        }
    }
}

impl fmt::Display for EntryType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            catalog::entry_meta::EntryType::from(*self).as_str_name()
        )
    }
}

/// Metadata associated with every entry in the catalog.
#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct EntryMeta {
    pub entry_type: EntryType,
    pub id: u32,
    pub parent: u32,
    pub name: String,
    pub builtin: bool,
}

impl From<EntryMeta> for catalog::EntryMeta {
    fn from(value: EntryMeta) -> Self {
        let entry_type: catalog::entry_meta::EntryType = value.entry_type.into();
        catalog::EntryMeta {
            entry_type: entry_type as i32,
            id: value.id,
            parent: value.parent,
            name: value.name,
            builtin: value.builtin,
        }
    }
}

impl TryFrom<catalog::EntryMeta> for EntryMeta {
    type Error = ProtoConvError;
    fn try_from(value: catalog::EntryMeta) -> Result<Self, Self::Error> {
        Ok(EntryMeta {
            entry_type: value.entry_type.try_into()?,
            id: value.id,
            parent: value.parent,
            name: value.name,
            builtin: value.builtin,
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct SchemaEntry {
    pub meta: EntryMeta,
}

impl TryFrom<catalog::SchemaEntry> for SchemaEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::SchemaEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(SchemaEntry { meta })
    }
}

impl From<SchemaEntry> for catalog::SchemaEntry {
    fn from(value: SchemaEntry) -> Self {
        catalog::SchemaEntry {
            meta: Some(value.meta.into()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TableEntry {
    pub meta: EntryMeta,
    pub columns: Vec<ColumnDefinition>,
}

impl TryFrom<catalog::TableEntry> for TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(TableEntry {
            meta,
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

impl TryFrom<TableEntry> for catalog::TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: TableEntry) -> Result<Self, Self::Error> {
        Ok(catalog::TableEntry {
            meta: Some(value.meta.into()),
            columns: value
                .columns
                .into_iter()
                .map(|col| col.try_into())
                .collect::<Result<_, _>>()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: String,
    pub nullable: bool,
    pub arrow_type: DataType,
}

impl TryFrom<catalog::ColumnDefinition> for ColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ColumnDefinition) -> Result<Self, Self::Error> {
        let arrow_type: DataType = value.arrow_type.as_ref().required("arrow_type")?;
        Ok(ColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type,
        })
    }
}

// TODO: Try to make this just `From`. Would require some additional conversions
// for the arrow types.
impl TryFrom<ColumnDefinition> for catalog::ColumnDefinition {
    type Error = ProtoConvError;
    fn try_from(value: ColumnDefinition) -> Result<Self, Self::Error> {
        let arrow_type = arrow::ArrowType::try_from(&value.arrow_type)?;
        Ok(catalog::ColumnDefinition {
            name: value.name,
            nullable: value.nullable,
            arrow_type: Some(arrow_type),
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ViewEntry {
    pub meta: EntryMeta,
    pub sql: String,
}

impl TryFrom<catalog::ViewEntry> for ViewEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ViewEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ViewEntry {
            meta,
            sql: value.sql,
        })
    }
}

impl From<ViewEntry> for catalog::ViewEntry {
    fn from(value: ViewEntry) -> Self {
        catalog::ViewEntry {
            meta: Some(value.meta.into()),
            sql: value.sql,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ConnectionEntry {
    pub meta: EntryMeta,
    pub options: ConnectionOptions,
}

impl TryFrom<catalog::ConnectionEntry> for ConnectionEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ConnectionEntry {
            meta,
            options: value.options.required("options")?,
        })
    }
}

impl From<ConnectionEntry> for catalog::ConnectionEntry {
    fn from(value: ConnectionEntry) -> Self {
        catalog::ConnectionEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ExternalTableEntry {
    pub meta: EntryMeta,
    pub connection_id: u32,
    pub options: TableOptions,
}

impl TryFrom<catalog::ExternalTableEntry> for ExternalTableEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ExternalTableEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ExternalTableEntry {
            meta,
            connection_id: value.connection_id,
            options: value.options.required("options")?,
        })
    }
}

impl From<ExternalTableEntry> for catalog::ExternalTableEntry {
    fn from(value: ExternalTableEntry) -> Self {
        catalog::ExternalTableEntry {
            meta: Some(value.meta.into()),
            connection_id: value.connection_id,
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum TableOptions {
    Debug(TableOptionsDebug),
    Postgres(TableOptionsPostgres),
}

impl TryFrom<catalog::table_options::Options> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::table_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::table_options::Options::Debug(v) => TableOptions::Debug(v.try_into()?),
            catalog::table_options::Options::Postgres(v) => TableOptions::Postgres(v.try_into()?),
        })
    }
}

impl TryFrom<catalog::TableOptions> for TableOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<TableOptions> for catalog::table_options::Options {
    fn from(value: TableOptions) -> Self {
        match value {
            TableOptions::Debug(v) => catalog::table_options::Options::Debug(v.into()),
            TableOptions::Postgres(v) => catalog::table_options::Options::Postgres(v.into()),
        }
    }
}

impl From<TableOptions> for catalog::TableOptions {
    fn from(value: TableOptions) -> Self {
        catalog::TableOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsDebug {
    pub table_type: String,
}

impl TryFrom<catalog::TableOptionsDebug> for TableOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsDebug) -> Result<Self, Self::Error> {
        Ok(TableOptionsDebug {
            table_type: value.table_type,
        })
    }
}

impl From<TableOptionsDebug> for catalog::TableOptionsDebug {
    fn from(value: TableOptionsDebug) -> Self {
        catalog::TableOptionsDebug {
            table_type: value.table_type,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct TableOptionsPostgres {
    pub schema: String,
    pub table: String,
}

impl TryFrom<catalog::TableOptionsPostgres> for TableOptionsPostgres {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableOptionsPostgres) -> Result<Self, Self::Error> {
        Ok(TableOptionsPostgres {
            schema: value.schema,
            table: value.table,
        })
    }
}

impl From<TableOptionsPostgres> for catalog::TableOptionsPostgres {
    fn from(value: TableOptionsPostgres) -> Self {
        catalog::TableOptionsPostgres {
            schema: value.schema,
            table: value.table,
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub enum ConnectionOptions {
    Debug(ConnectionOptionsDebug),
    Postgres(ConnectionOptionsPostgres),
}

impl TryFrom<catalog::connection_options::Options> for ConnectionOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::connection_options::Options) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::connection_options::Options::Debug(v) => {
                ConnectionOptions::Debug(v.try_into()?)
            }
            catalog::connection_options::Options::Postgres(v) => {
                ConnectionOptions::Postgres(v.try_into()?)
            }
        })
    }
}

impl TryFrom<catalog::ConnectionOptions> for ConnectionOptions {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptions) -> Result<Self, Self::Error> {
        value.options.required("options")
    }
}

impl From<ConnectionOptions> for catalog::connection_options::Options {
    fn from(value: ConnectionOptions) -> Self {
        match value {
            ConnectionOptions::Debug(v) => catalog::connection_options::Options::Debug(v.into()),
            ConnectionOptions::Postgres(v) => {
                catalog::connection_options::Options::Postgres(v.into())
            }
        }
    }
}

impl From<ConnectionOptions> for catalog::ConnectionOptions {
    fn from(value: ConnectionOptions) -> Self {
        catalog::ConnectionOptions {
            options: Some(value.into()),
        }
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsDebug {}

impl TryFrom<catalog::ConnectionOptionsDebug> for ConnectionOptionsDebug {
    type Error = ProtoConvError;
    fn try_from(_value: catalog::ConnectionOptionsDebug) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsDebug {})
    }
}

impl From<ConnectionOptionsDebug> for catalog::ConnectionOptionsDebug {
    fn from(_value: ConnectionOptionsDebug) -> Self {
        catalog::ConnectionOptionsDebug {}
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ConnectionOptionsPostgres {
    pub connection_string: String,
}

impl TryFrom<catalog::ConnectionOptionsPostgres> for ConnectionOptionsPostgres {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ConnectionOptionsPostgres) -> Result<Self, Self::Error> {
        Ok(ConnectionOptionsPostgres {
            connection_string: value.connection_string,
        })
    }
}

impl From<ConnectionOptionsPostgres> for catalog::ConnectionOptionsPostgres {
    fn from(value: ConnectionOptionsPostgres) -> Self {
        catalog::ConnectionOptionsPostgres {
            connection_string: value.connection_string,
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
        fn roundtrip_entry_type(expected in any::<EntryType>()) {
            let p: catalog::entry_meta::EntryType = expected.clone().into();
            let got: EntryType = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_entry_meta(expected in any::<EntryMeta>()) {
            let p: catalog::EntryMeta = expected.clone().into();
            let got: EntryMeta = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_table_options(expected in any::<TableOptions>()) {
            let p: catalog::TableOptions = expected.clone().into();
            let got: TableOptions = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }

    proptest! {
        #[test]
        fn roundtrip_connection_options(expected in any::<ConnectionOptions>()) {
            let p: catalog::ConnectionOptions = expected.clone().into();
            let got: ConnectionOptions = p.try_into().unwrap();
            assert_eq!(expected, got);
        }
    }
}
