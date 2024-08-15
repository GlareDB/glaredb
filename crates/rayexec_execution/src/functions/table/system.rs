use std::{collections::VecDeque, fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{
    database::{
        catalog::CatalogTx, catalog_entry::CatalogEntryType, memory_catalog::MemoryCatalog,
        AttachInfo, DatabaseContext,
    },
    storage::table_storage::{DataTable, DataTableScan, EmptyTableScan},
};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::{
    array::{Array, Utf8Array, ValuesBuffer, VarlenValuesBuffer},
    batch::Batch,
    datatype::DataType,
    field::{Field, Schema},
};
use rayexec_error::{OptionExt, RayexecError, Result};

use super::{PlannedTableFunction, TableFunction, TableFunctionArgs};

pub trait SystemFunctionImpl: Debug + Sync + Send + Copy + 'static {
    const NAME: &'static str;
    fn schema() -> Schema;
    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<Batch>;
}

pub type ListDatabases = SystemFunction<ListDatabasesImpl>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListDatabasesImpl;

impl SystemFunctionImpl for ListDatabasesImpl {
    const NAME: &'static str = "list_databases";

    fn schema() -> Schema {
        Schema::new([
            Field::new("database_name", DataType::Utf8, false),
            Field::new("datasource", DataType::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<Batch> {
        let mut database_names = VarlenValuesBuffer::default();
        let mut datasources = VarlenValuesBuffer::default();

        for (name, _catalog, info) in databases.drain(..) {
            database_names.push_value(name);
            datasources.push_value(info.map(|i| i.datasource).unwrap_or("default".to_string()));
        }

        Batch::try_new([
            Array::Utf8(Utf8Array::new(database_names, None)),
            Array::Utf8(Utf8Array::new(datasources, None)),
        ])
    }
}

pub type ListTables = SystemFunction<ListTablesImpl>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListTablesImpl;

impl SystemFunctionImpl for ListTablesImpl {
    const NAME: &'static str = "list_tables";

    fn schema() -> Schema {
        Schema::new([
            Field::new("database_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("table_name", DataType::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<Batch> {
        let mut database_names = VarlenValuesBuffer::default();
        let mut schema_names = VarlenValuesBuffer::default();
        let mut table_names = VarlenValuesBuffer::default();

        let database = databases.pop_front().required("database")?;

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, schema| {
            schema.for_each_entry(tx, &mut |_, entry| {
                if entry.entry_type() != CatalogEntryType::Table {
                    return Ok(());
                }

                database_names.push_value(&database.0);
                schema_names.push_value(schema_name);
                table_names.push_value(&entry.name);

                Ok(())
            })?;
            Ok(())
        })?;

        Batch::try_new([
            Array::Utf8(Utf8Array::new(database_names, None)),
            Array::Utf8(Utf8Array::new(schema_names, None)),
            Array::Utf8(Utf8Array::new(table_names, None)),
        ])
    }
}

pub type ListSchemas = SystemFunction<ListSchemasImpl>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListSchemasImpl;

impl SystemFunctionImpl for ListSchemasImpl {
    const NAME: &'static str = "list_schemas";

    fn schema() -> Schema {
        Schema::new([
            Field::new("database_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<Batch> {
        let mut database_names = VarlenValuesBuffer::default();
        let mut schema_names = VarlenValuesBuffer::default();

        let database = databases.pop_front().required("database")?;

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, _| {
            database_names.push_value(&database.0);
            schema_names.push_value(schema_name);
            Ok(())
        })?;

        Batch::try_new([
            Array::Utf8(Utf8Array::new(database_names, None)),
            Array::Utf8(Utf8Array::new(schema_names, None)),
        ])
    }
}

#[derive(Debug, Clone, Default, Copy, PartialEq, Eq)]
pub struct SystemFunction<F: SystemFunctionImpl> {
    _ty: PhantomData<F>,
}

impl<F: SystemFunctionImpl> SystemFunction<F> {
    pub const fn new() -> Self {
        SystemFunction { _ty: PhantomData }
    }
}

impl<F: SystemFunctionImpl> TableFunction for SystemFunction<F> {
    fn name(&self) -> &'static str {
        F::NAME
    }

    fn plan_and_initialize(
        &self,
        context: &DatabaseContext,
        _args: TableFunctionArgs,
    ) -> BoxFuture<'_, Result<Box<dyn PlannedTableFunction>>> {
        // TODO: Method on args returning an error if not empty.

        let databases = context
            .iter_databases()
            .map(|(name, database)| {
                (
                    name.clone(),
                    database.catalog.clone(),
                    database.attach_info.clone(),
                )
            })
            .collect();

        Box::pin(async move {
            Ok(Box::new(PlannedSystemFunction {
                databases,
                function: *self,
            }) as _)
        })
    }

    fn decode_state(&self, _state: &[u8]) -> Result<Box<dyn PlannedTableFunction>> {
        Ok(Box::new(PlannedSystemFunction {
            databases: Vec::new(),
            function: *self,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct PlannedSystemFunction<F: SystemFunctionImpl> {
    databases: Vec<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    function: SystemFunction<F>,
}

impl<F: SystemFunctionImpl> PlannedTableFunction for PlannedSystemFunction<F> {
    fn encode_state(&self, _state: &mut Vec<u8>) -> Result<()> {
        Ok(())
    }

    fn table_function(&self) -> &dyn TableFunction {
        &self.function
    }

    fn schema(&self) -> Schema {
        F::schema()
    }

    fn datatable(&self) -> Result<Box<dyn DataTable>> {
        Ok(Box::new(SystemDataTable {
            databases: Mutex::new(Some(self.databases.clone().into_iter().collect())),
            function: self.function,
        }))
    }
}

#[derive(Debug)]
struct SystemDataTable<F: SystemFunctionImpl> {
    #[allow(clippy::type_complexity)] // Temp
    databases: Mutex<Option<VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>>>,
    function: SystemFunction<F>,
}

impl<F: SystemFunctionImpl> DataTable for SystemDataTable<F> {
    fn scan(&self, num_partitions: usize) -> Result<Vec<Box<dyn DataTableScan>>> {
        let databases = self
            .databases
            .lock()
            .take()
            .ok_or_else(|| RayexecError::new("Scan called multiple times"))?;

        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(SystemDataTableScan {
            databases,
            _function: self.function,
        }) as _];

        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

#[derive(Debug)]
struct SystemDataTableScan<F: SystemFunctionImpl> {
    databases: VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    _function: SystemFunction<F>,
}

impl<F: SystemFunctionImpl> DataTableScan for SystemDataTableScan<F> {
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<Batch>>> {
        Box::pin(async {
            if self.databases.is_empty() {
                return Ok(None);
            }

            let batch = F::new_batch(&mut self.databases)?;

            Ok(Some(batch))
        })
    }
}
