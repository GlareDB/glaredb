use parking_lot::{Mutex, RwLock};
use rayexec_bullet::batch::Batch;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::database::catalog::{Catalog, CatalogTx};
use crate::database::create::{CreateScalarFunctionInfo, CreateTableInfo, OnConflict};
use crate::database::ddl::{CatalogModifier, CreateFut, DropFut};
use crate::database::entry::{CatalogEntry, TableEntry};
use crate::functions::aggregate::GenericAggregateFunction;
use crate::functions::scalar::GenericScalarFunction;
use crate::{
    database::table::{DataTable, DataTableInsert, DataTableScan},
    execution::operators::{PollPull, PollPush},
};

/// Quick and dirty in-memory implementation of a catalog and related data
/// tables.
///
/// This utilizes a few more locks than I'd like, however it should be good
/// enough for testing. In the future, modifications should be written to the
/// catalog tx then committed to the catalog at the end (without needing
/// interior mutability).
///
/// DDLs may seem unnecessarily complex right now with having to return a
/// "future" instead of just taking the lock and inserting th entry, but this is
/// exercising the functionality of executing DDL inside our scheduler. With
/// external data sources, these operations will be truly async, and so these
/// methods will make more sense then.
///
/// Actual storage is not transactional either.
#[derive(Debug)]
pub struct MemoryCatalog {
    inner: Arc<RwLock<MemorySchemas>>,
}

#[derive(Debug)]
struct MemorySchemas {
    schemas: HashMap<String, MemorySchema>,
}

#[derive(Debug, Default)]
struct MemorySchema {
    // TODO: OIDs
    // TODO: Seperate maps for funcs/tables
    entries: HashMap<String, CatalogEntry>,
    tables: HashMap<String, MemoryDataTable>,
}

impl MemoryCatalog {
    /// Creates a new memory catalog with a single named schema.
    pub fn new_with_temp_schema(schema: &str) -> Self {
        let mut schemas = HashMap::new();
        schemas.insert(schema.to_string(), MemorySchema::default());

        MemoryCatalog {
            inner: Arc::new(RwLock::new(MemorySchemas { schemas })),
        }
    }
}

impl Catalog for MemoryCatalog {
    fn get_table_entry(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        name: &str,
    ) -> Result<Option<TableEntry>> {
        let inner = self.inner.read();
        let schema = inner
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;

        match schema.entries.get(name) {
            Some(CatalogEntry::Table(ent)) => Ok(Some(ent.clone())),
            Some(_) => Err(RayexecError::new("Entry not a table")),
            None => Ok(None),
        }
    }

    fn get_scalar_fn(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<Box<dyn GenericScalarFunction>>> {
        unimplemented!()
    }

    fn get_aggregate_fn(
        &self,
        _tx: &CatalogTx,
        _schema: &str,
        _name: &str,
    ) -> Result<Option<Box<dyn GenericAggregateFunction>>> {
        unimplemented!()
    }

    fn data_table(
        &self,
        _tx: &CatalogTx,
        schema: &str,
        ent: &TableEntry,
    ) -> Result<Box<dyn DataTable>> {
        let inner = self.inner.read();
        let schema = inner
            .schemas
            .get(schema)
            .ok_or_else(|| RayexecError::new(format!("Missing schema: {schema}")))?;
        let table = schema
            .tables
            .get(&ent.name)
            .cloned()
            .ok_or_else(|| RayexecError::new(format!("Missing table: {}", ent.name)))?;

        Ok(Box::new(table) as _)
    }

    fn catalog_modifier(&self, _tx: &CatalogTx) -> Result<Box<dyn CatalogModifier>> {
        Ok(Box::new(MemoryCatalogModifier {
            inner: self.inner.clone(),
        }))
    }
}

#[derive(Debug)]
pub struct MemoryCatalogModifier {
    inner: Arc<RwLock<MemorySchemas>>,
}

impl CatalogModifier for MemoryCatalogModifier {
    fn create_schema(&self, name: &str) -> Result<Box<dyn CreateFut>> {
        Ok(Box::new(MemoryCreateSchema {
            schema: name.to_string(),
            inner: self.inner.clone(),
        }))
    }

    fn drop_schema(&self, _name: &str) -> Result<Box<dyn DropFut>> {
        unimplemented!()
    }

    fn create_table(&self, schema: &str, info: CreateTableInfo) -> Result<Box<dyn CreateFut>> {
        Ok(Box::new(MemoryCreateTable {
            schema: schema.to_string(),
            info,
            inner: self.inner.clone(),
        }))
    }

    fn drop_table(&self, _schema: &str, _name: &str) -> Result<Box<dyn DropFut>> {
        unimplemented!()
    }

    fn create_scalar_function(
        &self,
        _info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut>> {
        unimplemented!()
    }

    fn create_aggregate_function(
        &self,
        _info: CreateScalarFunctionInfo,
    ) -> Result<Box<dyn CreateFut>> {
        unimplemented!()
    }
}

#[derive(Debug)]
struct MemoryCreateSchema {
    schema: String,
    inner: Arc<RwLock<MemorySchemas>>,
}

impl CreateFut for MemoryCreateSchema {
    fn poll_create(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        let mut inner = self.inner.write();
        if inner.schemas.contains_key(&self.schema) {
            return Poll::Ready(Err(RayexecError::new(format!(
                "Schema already exists: {}",
                self.schema
            ))));
        }

        inner
            .schemas
            .insert(self.schema.clone(), MemorySchema::default());

        Poll::Ready(Ok(()))
    }
}

#[derive(Debug)]
struct MemoryCreateTable {
    schema: String,
    info: CreateTableInfo,
    inner: Arc<RwLock<MemorySchemas>>,
}

impl CreateFut for MemoryCreateTable {
    fn poll_create(&mut self, _cx: &mut Context) -> Poll<Result<()>> {
        let mut inner = self.inner.write();
        let schema = match inner.schemas.get_mut(&self.schema) {
            Some(schema) => schema,
            None => {
                return Poll::Ready(Err(RayexecError::new(format!(
                    "Missing schema: {}",
                    &self.schema
                ))))
            }
        };
        if schema.entries.contains_key(&self.info.name) {
            match self.info.on_conflict {
                OnConflict::Ignore => return Poll::Ready(Ok(())),
                OnConflict::Error => {
                    return Poll::Ready(Err(RayexecError::new(format!(
                        "Duplicate table name: {}",
                        self.info.name
                    ))))
                }
                OnConflict::Replace => (),
            }
        }

        schema.entries.insert(
            self.info.name.clone(),
            CatalogEntry::Table(TableEntry {
                name: self.info.name.clone(),
                columns: self.info.columns.clone(),
            }),
        );

        schema
            .tables
            .insert(self.info.name.clone(), MemoryDataTable::default());

        Poll::Ready(Ok(()))
    }
}

#[derive(Debug, Clone, Default)]
pub struct MemoryDataTable {
    data: Arc<Mutex<Vec<Batch>>>,
}

impl DataTable for MemoryDataTable {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let mut scans: Vec<_> = (0..num_partitions)
            .map(|_| MemoryDataTableScan { data: Vec::new() })
            .collect();

        let data = {
            let data = self.data.lock();
            data.clone()
        };

        for (idx, batch) in data.into_iter().enumerate() {
            scans[idx % num_partitions].data.push(batch);
        }

        Ok(scans
            .into_iter()
            .map(|scan| Box::new(scan) as Box<_>)
            .collect())
    }

    fn insert(&self, input_partitions: usize) -> Result<Vec<Box<dyn DataTableInsert>>> {
        let inserts: Vec<_> = (0..input_partitions)
            .map(|_| {
                Box::new(MemoryDataTableInsert {
                    collected: Vec::new(),
                    data: self.data.clone(),
                }) as _
            })
            .collect();

        Ok(inserts)
    }
}

#[derive(Debug)]
pub struct MemoryDataTableScan {
    data: Vec<Batch>,
}

impl DataTableScan for MemoryDataTableScan {
    fn poll_pull(&mut self, _cx: &mut Context) -> Result<PollPull> {
        match self.data.pop() {
            Some(batch) => Ok(PollPull::Batch(batch)),
            None => Ok(PollPull::Exhausted),
        }
    }
}

#[derive(Debug)]
pub struct MemoryDataTableInsert {
    collected: Vec<Batch>,
    data: Arc<Mutex<Vec<Batch>>>,
}

impl DataTableInsert for MemoryDataTableInsert {
    fn poll_push(&mut self, _cx: &mut Context, batch: Batch) -> Result<PollPush> {
        self.collected.push(batch);
        Ok(PollPush::NeedsMore)
    }

    fn finalize(&mut self) -> Result<()> {
        let mut data = self.data.lock();
        data.append(&mut self.collected);
        Ok(())
    }
}
