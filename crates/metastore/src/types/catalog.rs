use super::{FromOptionalField, ProtoConvError};
use crate::proto::arrow;
use crate::proto::catalog;
use crate::types::options::{DatabaseOptions, TableOptions};
use datafusion::arrow::datatypes::DataType;
use proptest_derive::Arbitrary;
use std::collections::HashMap;
use std::fmt;

#[derive(Debug, Clone)]
pub struct CatalogState {
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
            version: value.version,
            entries,
        })
    }
}

impl TryFrom<CatalogState> for catalog::CatalogState {
    type Error = ProtoConvError;
    fn try_from(value: CatalogState) -> Result<Self, Self::Error> {
        Ok(catalog::CatalogState {
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
    Database(DatabaseEntry),
    Schema(SchemaEntry),
    Table(TableEntry),
    View(ViewEntry),
}

impl CatalogEntry {
    pub const fn entry_type(&self) -> EntryType {
        match self {
            CatalogEntry::Database(_) => EntryType::Database,
            CatalogEntry::Schema(_) => EntryType::Schema,
            CatalogEntry::View(_) => EntryType::View,
            CatalogEntry::Table(_) => EntryType::Table,
        }
    }

    pub const fn is_database(&self) -> bool {
        matches!(self, CatalogEntry::Database(_))
    }

    pub const fn is_schema(&self) -> bool {
        matches!(self, CatalogEntry::Schema(_))
    }

    /// Get the entry metadata.
    pub fn get_meta(&self) -> &EntryMeta {
        match self {
            CatalogEntry::Database(db) => &db.meta,
            CatalogEntry::Schema(schema) => &schema.meta,
            CatalogEntry::View(view) => &view.meta,
            CatalogEntry::Table(table) => &table.meta,
        }
    }

    /// Get a mutable entry metadata referenuce.
    pub fn get_meta_mut(&mut self) -> &mut EntryMeta {
        match self {
            CatalogEntry::Database(db) => &mut db.meta,
            CatalogEntry::Schema(schema) => &mut schema.meta,
            CatalogEntry::View(view) => &mut view.meta,
            CatalogEntry::Table(table) => &mut table.meta,
        }
    }
}

impl TryFrom<catalog::catalog_entry::Entry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::catalog_entry::Entry) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::catalog_entry::Entry::Database(v) => CatalogEntry::Database(v.try_into()?),
            catalog::catalog_entry::Entry::Schema(v) => CatalogEntry::Schema(v.try_into()?),
            catalog::catalog_entry::Entry::Table(v) => CatalogEntry::Table(v.try_into()?),
            catalog::catalog_entry::Entry::View(v) => CatalogEntry::View(v.try_into()?),
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
            CatalogEntry::Database(v) => catalog::catalog_entry::Entry::Database(v.into()),
            CatalogEntry::Schema(v) => catalog::catalog_entry::Entry::Schema(v.into()),
            CatalogEntry::View(v) => catalog::catalog_entry::Entry::View(v.into()),
            CatalogEntry::Table(v) => catalog::catalog_entry::Entry::Table(v.try_into()?),
        };
        Ok(catalog::CatalogEntry { entry: Some(ent) })
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq)]
pub enum EntryType {
    Database,
    Schema,
    Table,
    View,
}

impl EntryType {
    pub fn as_str(&self) -> &'static str {
        match self {
            EntryType::Database => "database",
            EntryType::Schema => "schema",
            EntryType::Table => "table",
            EntryType::View => "view",
        }
    }
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
            catalog::entry_meta::EntryType::Database => EntryType::Database,
            catalog::entry_meta::EntryType::Schema => EntryType::Schema,
            catalog::entry_meta::EntryType::Table => EntryType::Table,
            catalog::entry_meta::EntryType::View => EntryType::View,
        })
    }
}

impl From<EntryType> for catalog::entry_meta::EntryType {
    fn from(value: EntryType) -> Self {
        match value {
            EntryType::Database => catalog::entry_meta::EntryType::Database,
            EntryType::Schema => catalog::entry_meta::EntryType::Schema,
            EntryType::Table => catalog::entry_meta::EntryType::Table,
            EntryType::View => catalog::entry_meta::EntryType::View,
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
    pub external: bool,
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
            external: value.external,
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
            external: value.external,
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct DatabaseEntry {
    pub meta: EntryMeta,
    pub options: DatabaseOptions,
}

impl TryFrom<catalog::DatabaseEntry> for DatabaseEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::DatabaseEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(DatabaseEntry {
            meta,
            options: value.options.required("options")?,
        })
    }
}

impl From<DatabaseEntry> for catalog::DatabaseEntry {
    fn from(value: DatabaseEntry) -> Self {
        catalog::DatabaseEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
        }
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
    pub options: TableOptions,
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
            options: value.options.required("options".to_string())?,
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
            options: Some(value.options.into()),
        })
    }
}

#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct ColumnDefinition {
    pub name: String,
    pub nullable: bool,
    // TODO: change proptest strategy to select random DataType
    #[proptest(value("DataType::Utf8"))]
    pub arrow_type: DataType,
}

impl ColumnDefinition {
    /// Create a vec of column definitions.
    ///
    /// Tuples are in the form of:
    /// (name, datatype, nullable)
    pub fn from_tuples<C, N>(cols: C) -> Vec<ColumnDefinition>
    where
        C: IntoIterator<Item = (N, DataType, bool)>,
        N: Into<String>,
    {
        cols.into_iter()
            .map(|(name, arrow_type, nullable)| ColumnDefinition {
                name: name.into(),
                nullable,
                arrow_type,
            })
            .collect()
    }
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
}
