use std::sync::Arc;

use rayexec_error::{RayexecError, Result};
use scc::ebr::Guard;

use super::catalog::CatalogTx;
use super::catalog_entry::{
    AggregateFunctionEntry,
    CatalogEntry,
    CatalogEntryInner,
    CatalogEntryType,
    CopyToFunctionEntry,
    ScalarFunctionEntry,
    SchemaEntry,
    TableEntry,
    TableFunctionEntry,
    ViewEntry,
};
use super::catalog_map::CatalogMap;
use super::create::{
    CreateAggregateFunctionInfo,
    CreateCopyToFunctionInfo,
    CreateScalarFunctionInfo,
    CreateSchemaInfo,
    CreateTableFunctionInfo,
    CreateTableInfo,
    CreateViewInfo,
};
use super::drop::{DropInfo, DropObject};
use crate::database::create::OnConflict;

// Using `scc` package for concurrent datastructures.
//
// Concurrency is needed since we're wrapping everything in arcs to allow lifetimes
// of entries to not rely on the database context (e.g. if we have entries on certain
// pipeline operators).
//
// `scc` has a neat property where hashmaps can be read without acquiring locks.
// This is beneficial for:
//
// - Having a single "memory" catalog implementation for real catalogs and
//   system catalogs. Function lookups don't need to acquire any locks. This is
//   good because we want to share a single read-only system catalog across all
//   sessions.
//
// However these methods require a Guard for EBR, but these don't actually
// matter for us. Any synchronization for data removal that's required for our
// use case will go through more typical transaction semantics with timestamps.
//
// I (Sean) opted for `scc` over DashMap primarily for the lock-free read-only
// properties. DashMap has a fixed number of shards, and any read will acquire a
// lock for that shard. `evmap` was an alternative with nice lock-free read
// properties, but `scc` seems more active.
//
// ---
//
// The memory catalog will be treated as an in-memory cache for external
// databases/catalogs with entries getting loaded in during binding. This lets
// us have consistent types for catalog/table access without requiring data
// sources implement that logic.

// TODO: Indicate "caching" variant when caching external catalog objects.

#[derive(Debug, Default)]
pub struct MemoryCatalog {
    schemas: scc::HashIndex<String, Arc<MemorySchema>>,
}

impl MemoryCatalog {
    pub fn get_schema(&self, _tx: &CatalogTx, name: &str) -> Result<Option<Arc<MemorySchema>>> {
        let guard = Guard::new();
        Ok(self.schemas.peek(name, &guard).cloned())
    }

    pub fn create_schema(
        &self,
        _tx: &CatalogTx,
        create: &CreateSchemaInfo,
    ) -> Result<Arc<MemorySchema>> {
        let schema = Arc::new(MemorySchema {
            schema: Arc::new(CatalogEntry {
                oid: 0,
                name: create.name.clone(),
                entry: CatalogEntryInner::Schema(SchemaEntry {}),
                child: None,
            }),
            tables: CatalogMap::default(),
            table_functions: CatalogMap::default(),
            functions: CatalogMap::default(),
            copy_to_functions: CatalogMap::default(),
        });

        use scc::hash_index::Entry;

        match (self.schemas.entry(create.name.clone()), create.on_conflict) {
            (Entry::Vacant(ent), _) => {
                ent.insert_entry(schema.clone());
                Ok(schema)
            }
            (Entry::Occupied(ent), OnConflict::Ignore) => {
                // Return existing entry.
                Ok(ent.get().clone())
            }
            (Entry::Occupied(ent), OnConflict::Replace) => {
                // TODO: Drop then replace.
                ent.update(schema.clone());
                Ok(schema)
            }
            (Entry::Occupied(_), OnConflict::Error) => Err(RayexecError::new(format!(
                "Duplicate schema name: '{}'",
                create.name,
            ))),
        }
    }

    pub fn drop_entry(&self, tx: &CatalogTx, drop: &DropInfo) -> Result<()> {
        if drop.object == DropObject::Schema {
            if drop.cascade {
                return Err(RayexecError::new("CASCADE not yet supported"));
            }

            // TODO: Schemas should be implemented as a CatalogMap.
            if !self.schemas.remove(&drop.schema) && !drop.if_exists {
                return Err(RayexecError::new(format!(
                    "Missing schema: {}",
                    drop.schema
                )));
            }

            return Ok(());
        }

        let schema = self
            .schemas
            .get(&drop.schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {}", drop.schema)))?;

        schema.drop_entry(tx, drop)?;

        Ok(())
    }

    pub fn for_each_schema<F>(&self, _tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &Arc<MemorySchema>) -> Result<()>,
    {
        let guard = Guard::new();
        for (name, schema) in self.schemas.iter(&guard) {
            func(name, schema)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemorySchema {
    /// Catalog entry representing this schema.
    schema: Arc<CatalogEntry>,
    /// All tables and views in the schema.
    tables: CatalogMap,
    /// All table functions in the schema.
    table_functions: CatalogMap,
    /// All scalar and aggregate functions in the schema.
    functions: CatalogMap,
    /// All functions implementing COPY TO for a fomat in the schema.
    copy_to_functions: CatalogMap,
}

impl MemorySchema {
    pub fn entry(&self) -> &Arc<CatalogEntry> {
        &self.schema
    }

    pub fn create_table(
        &self,
        tx: &CatalogTx,
        create: &CreateTableInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let table = CatalogEntry {
            oid: 0,
            name: create.name.clone(),
            entry: CatalogEntryInner::Table(TableEntry {
                columns: create.columns.clone(),
            }),
            child: None,
        };

        Self::create_entry(tx, &self.tables, table, create.on_conflict)
    }

    pub fn create_view(
        &self,
        tx: &CatalogTx,
        create: &CreateViewInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let view = CatalogEntry {
            oid: 0,
            name: create.name.clone(),
            entry: CatalogEntryInner::View(ViewEntry {
                column_aliases: create.column_aliases.clone(),
                query_sql: create.query_string.clone(),
            }),
            child: None,
        };

        Self::create_entry(tx, &self.tables, view, create.on_conflict)
    }

    pub fn create_scalar_function(
        &self,
        tx: &CatalogTx,
        create: &CreateScalarFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            oid: 0,
            name: create.name.clone(),
            entry: CatalogEntryInner::ScalarFunction(ScalarFunctionEntry {
                function: create.implementation.clone(),
            }),
            child: None,
        };

        Self::create_entry(tx, &self.functions, ent, create.on_conflict)
    }

    pub fn create_aggregate_function(
        &self,
        tx: &CatalogTx,
        create: &CreateAggregateFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            oid: 0,
            name: create.name.clone(),
            entry: CatalogEntryInner::AggregateFunction(AggregateFunctionEntry {
                function: create.implementation.clone(),
            }),
            child: None,
        };

        Self::create_entry(tx, &self.functions, ent, create.on_conflict)
    }

    pub fn create_table_function(
        &self,
        tx: &CatalogTx,
        create: &CreateTableFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            oid: 0,
            name: create.name.clone(),
            entry: CatalogEntryInner::TableFunction(TableFunctionEntry {
                function: create.implementation.clone(),
            }),
            child: None,
        };

        Self::create_entry(tx, &self.table_functions, ent, create.on_conflict)
    }

    pub fn create_copy_to_function(
        &self,
        tx: &CatalogTx,
        create: &CreateCopyToFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            oid: 0,
            name: create.name.clone(),
            entry: CatalogEntryInner::CopyToFunction(CopyToFunctionEntry {
                function: create.implementation.clone(),
                format: create.format.clone(),
            }),
            child: None,
        };

        Self::create_entry(tx, &self.copy_to_functions, ent, create.on_conflict)
    }

    /// Internal helper for inserting entries into the schema while obeying
    /// conflict rules.
    fn create_entry(
        tx: &CatalogTx,
        map: &CatalogMap,
        entry: CatalogEntry,
        on_conflict: OnConflict,
    ) -> Result<Arc<CatalogEntry>> {
        let name = entry.name.clone();

        match (on_conflict, map.get_entry(tx, &name)?) {
            (OnConflict::Ignore, Some(ent)) => {
                // Return existing entry.
                return Ok(ent.clone());
            }
            (OnConflict::Replace, _) => {
                // TODO: Drop
                map.create_entry(tx, entry)?;
            }
            (OnConflict::Error, Some(_)) => {
                return Err(RayexecError::new(format!(
                    "Duplicate entry: {}",
                    entry.name
                )))
            }
            (OnConflict::Error, None) | (OnConflict::Ignore, None) => {
                map.create_entry(tx, entry)?;
            }
        }

        let ent = map
            .get_entry(tx, &name)?
            .ok_or_else(|| RayexecError::new("Missing entry after create"))?;

        Ok(ent)
    }

    pub fn get_table_or_view(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        self.tables.get_entry(tx, name)
    }

    pub fn get_table_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        self.table_functions.get_entry(tx, name)
    }

    pub fn get_function(&self, tx: &CatalogTx, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        self.functions.get_entry(tx, name)
    }

    pub fn get_scalar_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.functions.get_entry(tx, name)?;

        let ent = ent.and_then(|ent| match &ent.entry {
            CatalogEntryInner::ScalarFunction(_) => Some(ent),
            _ => None,
        });

        Ok(ent)
    }

    pub fn get_aggregate_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.functions.get_entry(tx, name)?;
        let ent = ent.and_then(|ent| match &ent.entry {
            CatalogEntryInner::AggregateFunction(_) => Some(ent),
            _ => None,
        });

        Ok(ent)
    }

    pub fn get_copy_to_function(
        &self,
        tx: &CatalogTx,
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.copy_to_functions.get_entry(tx, name)?;
        let ent = ent.and_then(|ent| match &ent.entry {
            CatalogEntryInner::CopyToFunction(_) => Some(ent),
            _ => None,
        });

        Ok(ent)
    }

    pub fn get_copy_to_function_for_format(
        &self,
        tx: &CatalogTx,
        format: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let mut copy_to_ent = None;
        self.copy_to_functions
            .for_each_entry(tx, &mut |_, ent| match &ent.entry {
                CatalogEntryInner::CopyToFunction(copy) => {
                    if copy.format == format {
                        copy_to_ent = Some(ent.clone());
                    }
                    Ok(())
                }
                _ => unreachable!("copy_to_functions only contains copy to function entries"),
            })?;

        Ok(copy_to_ent)
    }

    pub fn drop_entry(&self, tx: &CatalogTx, drop: &DropInfo) -> Result<()> {
        match &drop.object {
            DropObject::Index(_) => Err(RayexecError::new("Dropping indexes not yet supported")),
            DropObject::Function(_) => {
                Err(RayexecError::new("Dropping functions not yet supported"))
            }
            DropObject::Table(name) => {
                Self::drop_entry_inner(tx, &self.tables, name, drop.if_exists, drop.cascade)
            }
            DropObject::View(name) => {
                Self::drop_entry_inner(tx, &self.tables, name, drop.if_exists, drop.cascade)
            }
            DropObject::Schema => Err(RayexecError::new("Cannot drop schema from inside schema")),
        }
    }

    fn drop_entry_inner(
        tx: &CatalogTx,
        map: &CatalogMap,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<()> {
        if cascade {
            return Err(RayexecError::new("CASCADE not yet supported"));
        }

        let ent = map.get_entry(tx, name)?;

        match (ent, if_exists) {
            (Some(ent), _) => {
                map.drop_entry(tx, ent.as_ref())?;
                Ok(())
            }
            (None, true) => Ok(()),
            (None, false) => Err(RayexecError::new("Missing entry, cannot drop")),
        }
    }

    pub fn for_each_entry<F>(&self, tx: &CatalogTx, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &Arc<CatalogEntry>) -> Result<()>,
    {
        self.tables.for_each_entry(tx, func)?;
        self.table_functions.for_each_entry(tx, func)?;
        self.functions.for_each_entry(tx, func)?;
        Ok(())
    }

    pub fn find_similar_entry(
        &self,
        tx: &CatalogTx,
        typs: &[CatalogEntryType],
        name: &str,
    ) -> Result<Option<SimilarEntry>> {
        let mut similar: Option<SimilarEntry> = None;

        for typ in typs {
            match typ {
                CatalogEntryType::Table => self.tables.for_each_entry(tx, &mut |_, ent| {
                    SimilarEntry::maybe_update(&mut similar, ent, name);
                    Ok(())
                })?,
                CatalogEntryType::ScalarFunction => {
                    self.functions.for_each_entry(tx, &mut |_, ent| {
                        SimilarEntry::maybe_update(&mut similar, ent, name);
                        Ok(())
                    })?
                }
                CatalogEntryType::AggregateFunction => {
                    self.functions.for_each_entry(tx, &mut |_, ent| {
                        SimilarEntry::maybe_update(&mut similar, ent, name);
                        Ok(())
                    })?
                }
                CatalogEntryType::TableFunction => {
                    self.table_functions.for_each_entry(tx, &mut |_, ent| {
                        SimilarEntry::maybe_update(&mut similar, ent, name);
                        Ok(())
                    })?
                }
                _ => (),
            }
        }

        Ok(similar)
    }
}

#[derive(Debug, Clone)]
pub struct SimilarEntry {
    pub score: f64,
    pub entry: Arc<CatalogEntry>,
}

impl SimilarEntry {
    /// Maybe updates `current` with a new entry if the new entry scores higher
    /// in similarity with `name`.
    fn maybe_update(current: &mut Option<Self>, entry: &Arc<CatalogEntry>, name: &str) {
        const SIMILARITY_THRESHOLD: f64 = 0.7;

        let score = strsim::jaro(&entry.name, name);
        if score > SIMILARITY_THRESHOLD {
            match current {
                Some(existing) => {
                    if score > existing.score {
                        *current = Some(SimilarEntry {
                            score,
                            entry: entry.clone(),
                        })
                    }
                }
                None => {
                    *current = Some(SimilarEntry {
                        score,
                        entry: entry.clone(),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::create::CreateAggregateFunctionInfo;
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;

    fn create_test_catalog() -> MemoryCatalog {
        let catalog = MemoryCatalog::default();
        let _schema = catalog
            .create_schema(
                &CatalogTx {},
                &CreateSchemaInfo {
                    name: "test".to_string(),
                    on_conflict: OnConflict::Error,
                },
            )
            .unwrap();

        catalog
    }

    #[test]
    fn similarity_function_name() {
        let catalog = create_test_catalog();
        let schema = catalog.get_schema(&CatalogTx {}, "test").unwrap().unwrap();

        schema
            .create_aggregate_function(
                &CatalogTx {},
                &CreateAggregateFunctionInfo {
                    name: "sum".to_string(),
                    implementation: FUNCTION_SET_SUM,
                    on_conflict: OnConflict::Error,
                },
            )
            .unwrap();

        let similar = schema
            .find_similar_entry(
                &CatalogTx {},
                &[CatalogEntryType::AggregateFunction],
                "summ",
            )
            .unwrap()
            .unwrap();
        assert_eq!("sum", similar.entry.name);

        let similar = schema
            .find_similar_entry(&CatalogTx {}, &[CatalogEntryType::AggregateFunction], "sim")
            .unwrap()
            .unwrap();
        assert_eq!("sum", similar.entry.name);

        let similar = schema
            .find_similar_entry(&CatalogTx {}, &[CatalogEntryType::AggregateFunction], "ham")
            .unwrap();
        assert!(similar.is_none());
    }
}
