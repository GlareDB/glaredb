use std::sync::Arc;

use futures::{stream, Stream};
use glaredb_error::{RayexecError, Result};
use scc::ebr::Guard;
use scc::HashIndex;

use super::create::{
    CreateAggregateFunctionInfo,
    CreateScalarFunctionInfo,
    CreateSchemaInfo,
    CreateTableFunctionInfo,
    CreateTableInfo,
    CreateViewInfo,
    OnConflict,
};
use super::drop::{DropInfo, DropObject};
use super::entry::{
    AggregateFunctionEntry,
    CatalogEntry,
    CatalogEntryInner,
    CatalogEntryType,
    ScalarFunctionEntry,
    TableEntry,
    TableFunctionEntry,
    ViewEntry,
};
use super::{Catalog, Schema};
use crate::catalog::entry::SchemaEntry;
use crate::execution::operators::catalog::create_schema::PhysicalCreateSchema;
use crate::execution::operators::catalog::create_table::PhysicalCreateTable;
use crate::execution::operators::catalog::create_table_as::PhysicalCreateTableAs;
use crate::execution::operators::catalog::create_view::PhysicalCreateView;
use crate::execution::operators::catalog::drop::PhysicalDrop;
use crate::execution::operators::catalog::insert::PhysicalInsert;
use crate::execution::operators::PlannedOperator;
use crate::functions::table::builtin::memory_scan::FUNCTION_SET_MEMORY_SCAN;
use crate::storage::storage_manager::{StorageManager, StorageTableId};

#[derive(Debug)]
pub struct MemoryCatalog {
    schemas: scc::HashIndex<String, Arc<MemorySchema>>,
}

impl MemoryCatalog {
    pub fn empty() -> Self {
        MemoryCatalog {
            schemas: HashIndex::new(),
        }
    }
}

impl Catalog for MemoryCatalog {
    type Schema = MemorySchema;

    fn create_schema(&self, create: &CreateSchemaInfo) -> Result<Arc<Self::Schema>> {
        let schema = Arc::new(MemorySchema {
            schema: Arc::new(CatalogEntry {
                name: create.name.clone(),
                entry: CatalogEntryInner::Schema(SchemaEntry {}),
                child: None,
            }),
            tables: CatalogMap::default(),
            table_functions: CatalogMap::default(),
            functions: CatalogMap::default(),
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

    fn get_schema(&self, name: &str) -> Result<Option<Arc<Self::Schema>>> {
        let guard = Guard::new();
        Ok(self.schemas.peek(name, &guard).cloned())
    }

    fn drop_entry(&self, drop: &DropInfo) -> Result<Option<Arc<CatalogEntry>>> {
        if drop.object == DropObject::Schema {
            if drop.cascade {
                return Err(RayexecError::new("CASCADE not yet supported"));
            }

            // TODO: Schemas should be implemented as a CatalogMap.

            // TODO: The separate clone here seems to be needed in order to
            // avoid deadlock...
            //
            // Check if this happens on the latest version of scc.
            let schema = self.schemas.get(&drop.schema).map(|ent| ent.clone());

            if !self.schemas.remove(&drop.schema) && !drop.if_exists {
                return Err(RayexecError::new(format!(
                    "Missing schema: {}",
                    drop.schema
                )));
            }

            return Ok(schema.map(|s| s.schema.clone()));
        }

        let schema = self
            .schemas
            .get(&drop.schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {}", drop.schema)))?;

        schema.drop_entry(drop)
    }

    fn plan_create_view(
        self: &Arc<Self>,
        schema: &str,
        create: CreateViewInfo,
    ) -> Result<PlannedOperator> {
        let schema = self.require_get_schema(schema)?;
        let operator = PhysicalCreateView {
            schema,
            info: create,
        };

        Ok(PlannedOperator::new_pull(operator))
    }

    fn plan_create_table(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator> {
        let schema = self.require_get_schema(schema)?;
        let operator = PhysicalCreateTable {
            storage: storage.clone(),
            schema,
            info: create,
        };

        Ok(PlannedOperator::new_pull(operator))
    }

    fn plan_insert(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        table: Arc<CatalogEntry>,
    ) -> Result<PlannedOperator> {
        Ok(PlannedOperator::new_execute(PhysicalInsert {
            storage: storage.clone(),
            entry: table,
        }))
    }

    fn plan_create_table_as(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        schema: &str,
        create: CreateTableInfo,
    ) -> Result<PlannedOperator> {
        let schema = self.require_get_schema(schema)?;
        let operator = PhysicalCreateTableAs {
            storage: storage.clone(),
            schema,
            info: create,
        };

        Ok(PlannedOperator::new_execute(operator))
    }

    fn plan_create_schema(self: &Arc<Self>, create: CreateSchemaInfo) -> Result<PlannedOperator> {
        Ok(PlannedOperator::new_pull(PhysicalCreateSchema {
            catalog: self.clone(),
            info: create,
        }))
    }

    fn plan_drop(
        self: &Arc<Self>,
        storage: &Arc<StorageManager>,
        drop: DropInfo,
    ) -> Result<PlannedOperator> {
        Ok(PlannedOperator::new_pull(PhysicalDrop {
            storage: storage.clone(),
            catalog: self.clone(),
            info: drop,
        }))
    }

    fn list_schemas(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<Self::Schema>>>> + Sync + Send + 'static {
        // TODO: Don't care yet.
        let g = Guard::new();
        let schemas: Vec<_> = self.schemas.iter(&g).map(|(_, v)| v.clone()).collect();
        stream::once(async move { Ok(schemas) })
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
    // /// All functions implementing COPY TO for a fomat in the schema.
    // copy_to_functions: CatalogMap,
}

impl Schema for MemorySchema {
    fn as_entry(&self) -> Arc<CatalogEntry> {
        self.schema.clone()
    }

    fn create_table(
        &self,
        create: &CreateTableInfo,
        storage_id: StorageTableId,
    ) -> Result<Arc<CatalogEntry>> {
        let table = CatalogEntry {
            name: create.name.clone(),
            entry: CatalogEntryInner::Table(TableEntry {
                columns: create.columns.clone(),
                function: FUNCTION_SET_MEMORY_SCAN,
                storage_id,
            }),
            child: None,
        };

        Self::create_entry(&self.tables, table, create.on_conflict)
    }

    fn create_view(&self, create: &CreateViewInfo) -> Result<Arc<CatalogEntry>> {
        let view = CatalogEntry {
            name: create.name.clone(),
            entry: CatalogEntryInner::View(ViewEntry {
                column_aliases: create.column_aliases.clone(),
                query_sql: create.query_string.clone(),
            }),
            child: None,
        };

        Self::create_entry(&self.tables, view, create.on_conflict)
    }

    fn create_scalar_function(
        &self,
        create: &CreateScalarFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            name: create.name.clone(),
            entry: CatalogEntryInner::ScalarFunction(ScalarFunctionEntry {
                function: create.implementation,
            }),
            child: None,
        };

        Self::create_entry(&self.functions, ent, create.on_conflict)
    }

    fn create_aggregate_function(
        &self,
        create: &CreateAggregateFunctionInfo,
    ) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            name: create.name.clone(),
            entry: CatalogEntryInner::AggregateFunction(AggregateFunctionEntry {
                function: create.implementation,
            }),
            child: None,
        };

        Self::create_entry(&self.functions, ent, create.on_conflict)
    }

    fn create_table_function(&self, create: &CreateTableFunctionInfo) -> Result<Arc<CatalogEntry>> {
        let ent = CatalogEntry {
            name: create.name.clone(),
            entry: CatalogEntryInner::TableFunction(TableFunctionEntry {
                function: create.implementation,
            }),
            child: None,
        };

        Self::create_entry(&self.table_functions, ent, create.on_conflict)
    }

    fn get_table_or_view(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        self.tables.get_entry(name)
    }

    fn get_table_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        self.table_functions.get_entry(name)
    }

    fn get_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        self.functions.get_entry(name)
    }

    fn get_scalar_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.functions.get_entry(name)?;

        let ent = ent.and_then(|ent| match &ent.entry {
            CatalogEntryInner::ScalarFunction(_) => Some(ent),
            _ => None,
        });

        Ok(ent)
    }

    fn get_aggregate_function(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        let ent = self.functions.get_entry(name)?;
        let ent = ent.and_then(|ent| match &ent.entry {
            CatalogEntryInner::AggregateFunction(_) => Some(ent),
            _ => None,
        });

        Ok(ent)
    }

    fn find_similar_entry(
        &self,
        entry_types: &[CatalogEntryType],
        name: &str,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        let mut similar: Option<SimilarEntry> = None;

        for typ in entry_types {
            match typ {
                CatalogEntryType::Table => self.tables.for_each_entry(&mut |_, ent| {
                    SimilarEntry::maybe_update(&mut similar, ent, name);
                    Ok(())
                })?,
                CatalogEntryType::ScalarFunction => {
                    self.functions.for_each_entry(&mut |_, ent| {
                        SimilarEntry::maybe_update(&mut similar, ent, name);
                        Ok(())
                    })?
                }
                CatalogEntryType::AggregateFunction => {
                    self.functions.for_each_entry(&mut |_, ent| {
                        SimilarEntry::maybe_update(&mut similar, ent, name);
                        Ok(())
                    })?
                }
                CatalogEntryType::TableFunction => {
                    self.table_functions.for_each_entry(&mut |_, ent| {
                        SimilarEntry::maybe_update(&mut similar, ent, name);
                        Ok(())
                    })?
                }
                _ => (),
            }
        }

        Ok(similar.map(|similar| similar.entry))
    }

    fn list_tables(
        self: &Arc<Self>,
    ) -> impl Stream<Item = Result<Vec<Arc<CatalogEntry>>>> + Sync + Send + 'static {
        // TODO: Also don't care here.
        let g = Guard::new();
        let tables: Vec<_> = self
            .tables
            .entries
            .iter(&g)
            .filter_map(|(_, v)| {
                if v.entry_type() == CatalogEntryType::Table {
                    Some(v.clone())
                } else {
                    None
                }
            })
            .collect();
        stream::once(async move { Ok(tables) })
    }
}

impl MemorySchema {
    /// Internal helper for inserting entries into the schema while obeying
    /// conflict rules.
    fn create_entry(
        map: &CatalogMap,
        entry: CatalogEntry,
        on_conflict: OnConflict,
    ) -> Result<Arc<CatalogEntry>> {
        let name = entry.name.clone();

        match (on_conflict, map.get_entry(&name)?) {
            (OnConflict::Ignore, Some(ent)) => {
                // Return existing entry.
                return Ok(ent.clone());
            }
            (OnConflict::Replace, _) => {
                // TODO: Drop
                map.create_entry(entry)?;
            }
            (OnConflict::Error, Some(_)) => {
                return Err(RayexecError::new(format!(
                    "Duplicate entry: {}",
                    entry.name
                )))
            }
            (OnConflict::Error, None) | (OnConflict::Ignore, None) => {
                map.create_entry(entry)?;
            }
        }

        let ent = map
            .get_entry(&name)?
            .ok_or_else(|| RayexecError::new("Missing entry after create"))?;

        Ok(ent)
    }

    fn drop_entry(&self, drop: &DropInfo) -> Result<Option<Arc<CatalogEntry>>> {
        match &drop.object {
            DropObject::Index(_) => Err(RayexecError::new("Dropping indexes not yet supported")),
            DropObject::Function(_) => {
                Err(RayexecError::new("Dropping functions not yet supported"))
            }
            DropObject::Table(name) => {
                Self::drop_entry_inner(&self.tables, name, drop.if_exists, drop.cascade)
            }
            DropObject::View(name) => {
                Self::drop_entry_inner(&self.tables, name, drop.if_exists, drop.cascade)
            }
            DropObject::Schema => Err(RayexecError::new("Cannot drop schema from inside schema")),
        }
    }

    fn drop_entry_inner(
        map: &CatalogMap,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Option<Arc<CatalogEntry>>> {
        if cascade {
            return Err(RayexecError::new("CASCADE not yet supported"));
        }

        let ent = map.get_entry(name)?;

        match (ent, if_exists) {
            (Some(ent), _) => {
                map.drop_entry(ent.as_ref())?;
                Ok(Some(ent))
            }
            (None, true) => Ok(None),
            (None, false) => Err(RayexecError::new("Missing entry, cannot drop")),
        }
    }
}

#[derive(Debug, Clone)]
struct SimilarEntry {
    score: f64,
    entry: Arc<CatalogEntry>,
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

/// Maps a name to some catalog entry.
#[derive(Debug, Default)]
struct CatalogMap {
    entries: scc::HashIndex<String, Arc<CatalogEntry>>,
}

impl CatalogMap {
    fn create_entry(&self, entry: CatalogEntry) -> Result<()> {
        match self.entries.entry(entry.name.clone()) {
            scc::hash_index::Entry::Occupied(ent) => Err(RayexecError::new(format!(
                "Duplicate entry name '{}'",
                ent.name
            ))),
            scc::hash_index::Entry::Vacant(ent) => {
                ent.insert_entry(Arc::new(entry));
                Ok(())
            }
        }
    }

    fn drop_entry(&self, entry: &CatalogEntry) -> Result<()> {
        if !self.entries.remove(&entry.name) {
            return Err(RayexecError::new(format!("Missing entry '{}'", entry.name)));
        }
        Ok(())
    }

    fn get_entry(&self, name: &str) -> Result<Option<Arc<CatalogEntry>>> {
        let guard = Guard::new();
        let ent = self.entries.peek(name, &guard).cloned();
        Ok(ent)
    }

    fn for_each_entry<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&String, &Arc<CatalogEntry>) -> Result<()>,
    {
        let guard = Guard::new();
        for (name, ent) in self.entries.iter(&guard) {
            func(name, ent)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::functions::aggregate::builtin::sum::FUNCTION_SET_SUM;

    fn create_test_catalog() -> MemoryCatalog {
        let catalog = MemoryCatalog::empty();
        let _schema = catalog
            .create_schema(&CreateSchemaInfo {
                name: "test".to_string(),
                on_conflict: OnConflict::Error,
            })
            .unwrap();

        catalog
    }

    #[test]
    fn similarity_function_name() {
        let catalog = create_test_catalog();
        let schema = catalog.get_schema("test").unwrap().unwrap();

        schema
            .create_aggregate_function(&CreateAggregateFunctionInfo {
                name: "sum".to_string(),
                implementation: FUNCTION_SET_SUM,
                on_conflict: OnConflict::Error,
            })
            .unwrap();

        let similar = schema
            .find_similar_entry(&[CatalogEntryType::AggregateFunction], "summ")
            .unwrap()
            .unwrap();
        assert_eq!("sum", similar.name);

        let similar = schema
            .find_similar_entry(&[CatalogEntryType::AggregateFunction], "sim")
            .unwrap()
            .unwrap();
        assert_eq!("sum", similar.name);

        let similar = schema
            .find_similar_entry(&[CatalogEntryType::AggregateFunction], "ham")
            .unwrap();
        assert!(similar.is_none());
    }
}
