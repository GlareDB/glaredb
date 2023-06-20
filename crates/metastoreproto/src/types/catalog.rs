use super::options::{
    CredentialsOptions, InternalColumnDefinition, TableOptionsInternal, TunnelOptions,
};
use super::{FromOptionalField, ProtoConvError};
use crate::proto::catalog;
use crate::types::options::{DatabaseOptions, TableOptions};
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
    Tunnel(TunnelEntry),
    Function(FunctionEntry),
    Credentials(CredentialsEntry),
}

impl CatalogEntry {
    pub const fn entry_type(&self) -> EntryType {
        match self {
            CatalogEntry::Database(_) => EntryType::Database,
            CatalogEntry::Schema(_) => EntryType::Schema,
            CatalogEntry::View(_) => EntryType::View,
            CatalogEntry::Table(_) => EntryType::Table,
            CatalogEntry::Tunnel(_) => EntryType::Tunnel,
            CatalogEntry::Function(_) => EntryType::Function,
            CatalogEntry::Credentials(_) => EntryType::Credentials,
        }
    }

    /// Get the entry metadata.
    pub fn get_meta(&self) -> &EntryMeta {
        match self {
            CatalogEntry::Database(db) => &db.meta,
            CatalogEntry::Schema(schema) => &schema.meta,
            CatalogEntry::View(view) => &view.meta,
            CatalogEntry::Table(table) => &table.meta,
            CatalogEntry::Tunnel(tunnel) => &tunnel.meta,
            CatalogEntry::Function(func) => &func.meta,
            CatalogEntry::Credentials(creds) => &creds.meta,
        }
    }

    /// Get a mutable entry metadata referenuce.
    pub fn get_meta_mut(&mut self) -> &mut EntryMeta {
        match self {
            CatalogEntry::Database(db) => &mut db.meta,
            CatalogEntry::Schema(schema) => &mut schema.meta,
            CatalogEntry::View(view) => &mut view.meta,
            CatalogEntry::Table(table) => &mut table.meta,
            CatalogEntry::Tunnel(tunnel) => &mut tunnel.meta,
            CatalogEntry::Function(func) => &mut func.meta,
            CatalogEntry::Credentials(creds) => &mut creds.meta,
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
            catalog::catalog_entry::Entry::Tunnel(v) => CatalogEntry::Tunnel(v.try_into()?),
            catalog::catalog_entry::Entry::Function(v) => CatalogEntry::Function(v.try_into()?),
            catalog::catalog_entry::Entry::Credentials(v) => {
                CatalogEntry::Credentials(v.try_into()?)
            }
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
            CatalogEntry::Tunnel(v) => catalog::catalog_entry::Entry::Tunnel(v.into()),
            CatalogEntry::Function(v) => catalog::catalog_entry::Entry::Function(v.into()),
            CatalogEntry::Credentials(v) => catalog::catalog_entry::Entry::Credentials(v.into()),
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
    Tunnel,
    Function,
    Credentials,
}

impl EntryType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            EntryType::Database => "database",
            EntryType::Schema => "schema",
            EntryType::Table => "table",
            EntryType::View => "view",
            EntryType::Tunnel => "tunnel",
            EntryType::Function => "function",
            EntryType::Credentials => "credentials",
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
            catalog::entry_meta::EntryType::Tunnel => EntryType::Tunnel,
            catalog::entry_meta::EntryType::Function => EntryType::Function,
            catalog::entry_meta::EntryType::Credentials => EntryType::Credentials,
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
            EntryType::Tunnel => catalog::entry_meta::EntryType::Tunnel,
            EntryType::Function => catalog::entry_meta::EntryType::Function,
            EntryType::Credentials => catalog::entry_meta::EntryType::Credentials,
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
    pub tunnel_id: Option<u32>,
}

impl TryFrom<catalog::DatabaseEntry> for DatabaseEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::DatabaseEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(DatabaseEntry {
            meta,
            options: value.options.required("options")?,
            tunnel_id: value.tunnel_id,
        })
    }
}

impl From<DatabaseEntry> for catalog::DatabaseEntry {
    fn from(value: DatabaseEntry) -> Self {
        catalog::DatabaseEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
            tunnel_id: value.tunnel_id,
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
    pub options: TableOptions,
    pub tunnel_id: Option<u32>,
}

impl TableEntry {
    /// Try to get the columns for this table if available.
    pub fn get_internal_columns(&self) -> Option<&[InternalColumnDefinition]> {
        match &self.options {
            TableOptions::Internal(TableOptionsInternal { columns, .. }) => Some(columns),
            _ => None,
        }
    }
}

impl TryFrom<catalog::TableEntry> for TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TableEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(TableEntry {
            meta,
            options: value.options.required("options".to_string())?,
            tunnel_id: value.tunnel_id,
        })
    }
}

impl TryFrom<TableEntry> for catalog::TableEntry {
    type Error = ProtoConvError;
    fn try_from(value: TableEntry) -> Result<Self, Self::Error> {
        Ok(catalog::TableEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.try_into()?),
            tunnel_id: value.tunnel_id,
        })
    }
}

impl fmt::Display for TableEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.meta.name, self.options.as_str())
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct ViewEntry {
    pub meta: EntryMeta,
    pub sql: String,
    pub columns: Vec<String>,
}

impl TryFrom<catalog::ViewEntry> for ViewEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::ViewEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(ViewEntry {
            meta,
            sql: value.sql,
            columns: value.columns,
        })
    }
}

impl From<ViewEntry> for catalog::ViewEntry {
    fn from(value: ViewEntry) -> Self {
        catalog::ViewEntry {
            meta: Some(value.meta.into()),
            sql: value.sql,
            columns: value.columns,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct TunnelEntry {
    pub meta: EntryMeta,
    pub options: TunnelOptions,
}

impl TryFrom<catalog::TunnelEntry> for TunnelEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::TunnelEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(TunnelEntry {
            meta,
            options: value.options.required("options")?,
        })
    }
}

impl From<TunnelEntry> for catalog::TunnelEntry {
    fn from(value: TunnelEntry) -> Self {
        catalog::TunnelEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, Arbitrary)]
pub enum FunctionType {
    Aggregate,
    Scalar,
    TableReturning,
}

impl FunctionType {
    pub const fn as_str(&self) -> &'static str {
        match self {
            FunctionType::Aggregate => "aggregate",
            FunctionType::Scalar => "scalar",
            FunctionType::TableReturning => "table",
        }
    }
}

impl TryFrom<i32> for FunctionType {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        catalog::function_entry::FunctionType::from_i32(value)
            .ok_or(ProtoConvError::UnknownEnumVariant("FunctionType", value))
            .and_then(|t| t.try_into())
    }
}

impl TryFrom<catalog::function_entry::FunctionType> for FunctionType {
    type Error = ProtoConvError;
    fn try_from(value: catalog::function_entry::FunctionType) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::function_entry::FunctionType::Unknown => {
                return Err(ProtoConvError::ZeroValueEnumVariant("FunctionType"))
            }
            catalog::function_entry::FunctionType::Aggregate => FunctionType::Aggregate,
            catalog::function_entry::FunctionType::Scalar => FunctionType::Scalar,
            catalog::function_entry::FunctionType::TableReturning => FunctionType::TableReturning,
        })
    }
}

impl From<FunctionType> for catalog::function_entry::FunctionType {
    fn from(value: FunctionType) -> Self {
        match value {
            FunctionType::Aggregate => catalog::function_entry::FunctionType::Aggregate,
            FunctionType::Scalar => catalog::function_entry::FunctionType::Scalar,
            FunctionType::TableReturning => catalog::function_entry::FunctionType::TableReturning,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FunctionEntry {
    pub meta: EntryMeta,
    pub func_type: FunctionType,
}

impl TryFrom<catalog::FunctionEntry> for FunctionEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::FunctionEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(FunctionEntry {
            meta,
            func_type: value.func_type.try_into()?,
        })
    }
}

impl From<FunctionEntry> for catalog::FunctionEntry {
    fn from(value: FunctionEntry) -> Self {
        let func_type: catalog::function_entry::FunctionType = value.func_type.into();
        catalog::FunctionEntry {
            meta: Some(value.meta.into()),
            func_type: func_type as i32,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct CredentialsEntry {
    pub meta: EntryMeta,
    pub options: CredentialsOptions,
    pub comment: String,
}

impl TryFrom<catalog::CredentialsEntry> for CredentialsEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CredentialsEntry) -> Result<Self, Self::Error> {
        let meta: EntryMeta = value.meta.required("meta")?;
        Ok(CredentialsEntry {
            meta,
            options: value.options.required("options")?,
            comment: value.comment,
        })
    }
}

impl From<CredentialsEntry> for catalog::CredentialsEntry {
    fn from(value: CredentialsEntry) -> Self {
        catalog::CredentialsEntry {
            meta: Some(value.meta.into()),
            options: Some(value.options.into()),
            comment: value.comment,
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
