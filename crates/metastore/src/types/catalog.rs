use super::{FromOptionalField, ProtoConvError};
use crate::proto::catalog;
use proptest_derive::Arbitrary;
use std::collections::HashMap;
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
            db_id: Uuid::parse_str(&value.db_id)?,
            version: value.version,
            entries,
        })
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub enum CatalogEntry {
    Schema(SchemaEntry),
}

impl CatalogEntry {
    pub const fn entry_type(&self) -> EntryType {
        match self {
            CatalogEntry::Schema(_) => EntryType::Schema,
        }
    }

    pub const fn is_schema(&self) -> bool {
        matches!(self, CatalogEntry::Schema(_))
    }

    /// Get the entry metadata.
    pub fn get_meta(&self) -> &EntryMeta {
        match self {
            CatalogEntry::Schema(schema) => &schema.meta,
        }
    }
}

impl TryFrom<catalog::catalog_entry::Entry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::catalog_entry::Entry) -> Result<Self, Self::Error> {
        Ok(match value {
            catalog::catalog_entry::Entry::Schema(v) => CatalogEntry::Schema(v.try_into()?),
            _ => todo!(),
        })
    }
}

impl TryFrom<catalog::CatalogEntry> for CatalogEntry {
    type Error = ProtoConvError;
    fn try_from(value: catalog::CatalogEntry) -> Result<Self, Self::Error> {
        value.entry.required("entry")
    }
}

#[derive(Debug, Clone, Copy, Arbitrary, PartialEq, Eq)]
pub enum EntryType {
    Schema,
    Table,
    View,
    Connection,
}

impl TryFrom<i32> for EntryType {
    type Error = ProtoConvError;
    fn try_from(value: i32) -> Result<Self, Self::Error> {
        catalog::entry_meta::EntryType::from_i32(value)
            .ok_or_else(|| ProtoConvError::UnknownEnumVariant("EntryType", value))
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
            EntryType::View => catalog::entry_meta::EntryType::View,
            EntryType::Connection => catalog::entry_meta::EntryType::Connection,
        }
    }
}

/// Metadata associated with every entry in the catalog.
#[derive(Debug, Clone, Arbitrary, PartialEq, Eq)]
pub struct EntryMeta {
    pub entry_type: EntryType,
    pub id: u32,
    pub parent: u32,
    pub name: String,
}

impl From<EntryMeta> for catalog::EntryMeta {
    fn from(value: EntryMeta) -> Self {
        let entry_type: catalog::entry_meta::EntryType = value.entry_type.into();
        catalog::EntryMeta {
            entry_type: entry_type as i32,
            id: value.id,
            parent: value.parent,
            name: value.name,
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
