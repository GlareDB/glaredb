use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::datatype::{DataType, DataTypeId};
use rayexec_bullet::executor::builder::{ArrayDataBuffer, GermanVarlenBuffer};
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_bullet::storage::GermanVarlenStorage;
use rayexec_error::{OptionExt, RayexecError, Result};

use crate::database::catalog::CatalogTx;
use crate::database::catalog_entry::{CatalogEntryInner, CatalogEntryType};
use crate::database::memory_catalog::MemoryCatalog;
use crate::database::{AttachInfo, DatabaseContext};
use crate::expr;
use crate::functions::table::{
    PlannedTableFunction,
    ScanPlanner,
    TableFunction,
    TableFunctionImpl,
    TableFunctionPlanner,
};
use crate::functions::{FunctionInfo, Signature};
use crate::logical::statistics::StatisticsValue;
use crate::storage::table_storage::{
    DataTable,
    DataTableScan,
    EmptyTableScan,
    ProjectedScan,
    Projections,
};

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
        let len = databases.len();

        let mut database_names = GermanVarlenBuffer::<str>::with_len(len);
        let mut datasources = GermanVarlenBuffer::<str>::with_len(len);

        for (idx, (name, _catalog, info)) in databases.drain(..).enumerate() {
            database_names.put(idx, name.as_str());
            datasources.put(
                idx,
                info.map(|i| i.datasource)
                    .unwrap_or("default".to_string())
                    .as_str(),
            );
        }

        Batch::try_new([
            Array::new_with_array_data(DataType::Utf8, database_names.into_data()),
            Array::new_with_array_data(DataType::Utf8, datasources.into_data()),
        ])
    }
}

pub type ListFunctions = SystemFunction<ListFunctionsImpl>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListFunctionsImpl;

impl SystemFunctionImpl for ListFunctionsImpl {
    const NAME: &'static str = "list_functions";

    fn schema() -> Schema {
        Schema::new([
            Field::new("database_name", DataType::Utf8, false),
            Field::new("schema_name", DataType::Utf8, false),
            Field::new("function_name", DataType::Utf8, false),
            Field::new("function_type", DataType::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<Batch> {
        let database = databases.pop_front().required("database")?;

        let mut database_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut schema_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut function_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut function_types = GermanVarlenStorage::with_metadata_capacity(0);

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, schema| {
            schema.for_each_entry(tx, &mut |_, entry| {
                match &entry.entry {
                    CatalogEntryInner::ScalarFunction(_) => {
                        database_names.try_push(database.0.as_bytes())?;
                        schema_names.try_push(schema_name.as_bytes())?;
                        function_names.try_push(entry.name.as_bytes())?;
                        function_types.try_push("scalar".as_bytes())?;
                    }
                    CatalogEntryInner::AggregateFunction(_) => {
                        database_names.try_push(database.0.as_bytes())?;
                        schema_names.try_push(schema_name.as_bytes())?;
                        function_names.try_push(entry.name.as_bytes())?;
                        function_types.try_push("aggregate".as_bytes())?;
                    }
                    CatalogEntryInner::TableFunction(_) => {
                        database_names.try_push(database.0.as_bytes())?;
                        schema_names.try_push(schema_name.as_bytes())?;
                        function_names.try_push(entry.name.as_bytes())?;
                        function_types.try_push("table".as_bytes())?;
                    }
                    _ => (),
                }

                Ok(())
            })?;
            Ok(())
        })?;

        Batch::try_new([
            Array::new_with_array_data(DataType::Utf8, database_names),
            Array::new_with_array_data(DataType::Utf8, schema_names),
            Array::new_with_array_data(DataType::Utf8, function_names),
            Array::new_with_array_data(DataType::Utf8, function_types),
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
        let database = databases.pop_front().required("database")?;

        let mut database_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut schema_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut table_names = GermanVarlenStorage::with_metadata_capacity(0);

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, schema| {
            schema.for_each_entry(tx, &mut |_, entry| {
                if entry.entry_type() != CatalogEntryType::Table {
                    return Ok(());
                }

                database_names.try_push(database.0.as_bytes())?;
                schema_names.try_push(schema_name.as_bytes())?;
                table_names.try_push(entry.name.as_bytes())?;

                Ok(())
            })?;
            Ok(())
        })?;

        Batch::try_new([
            Array::new_with_array_data(DataType::Utf8, database_names),
            Array::new_with_array_data(DataType::Utf8, schema_names),
            Array::new_with_array_data(DataType::Utf8, table_names),
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
        let database = databases.pop_front().required("database")?;

        let mut database_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut schema_names = GermanVarlenStorage::with_metadata_capacity(0);

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, _| {
            database_names.try_push(database.0.as_bytes())?;
            schema_names.try_push(schema_name.as_bytes())?;

            Ok(())
        })?;

        Batch::try_new([
            Array::new_with_array_data(DataType::Utf8, database_names),
            Array::new_with_array_data(DataType::Utf8, schema_names),
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

impl<F: SystemFunctionImpl> FunctionInfo for SystemFunction<F> {
    fn name(&self) -> &'static str {
        F::NAME
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[],
            variadic_arg: None,
            return_type: DataTypeId::Any,
            doc: None,
        }]
    }
}

impl<F: SystemFunctionImpl> TableFunction for SystemFunction<F> {
    fn planner(&self) -> TableFunctionPlanner {
        TableFunctionPlanner::Scan(&SystemFunctionPlanner::<F> { _f: PhantomData })
    }
}

#[derive(Debug, Clone)]
pub struct SystemFunctionPlanner<F: SystemFunctionImpl> {
    _f: PhantomData<F>,
}

impl<F> ScanPlanner for SystemFunctionPlanner<F>
where
    F: SystemFunctionImpl,
{
    fn plan<'a>(
        &self,
        context: &'a DatabaseContext,
        positional_inputs: Vec<OwnedScalarValue>,
        named_inputs: HashMap<String, OwnedScalarValue>,
    ) -> BoxFuture<'a, Result<PlannedTableFunction>> {
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

        let planned = PlannedTableFunction {
            function: Box::new(SystemFunction::<F>::new()),
            positional_inputs: positional_inputs.into_iter().map(expr::lit).collect(),
            named_inputs,
            function_impl: TableFunctionImpl::Scan(Arc::new(SystemDataTable::<F> {
                databases: Arc::new(Mutex::new(Some(databases))),
                _f: PhantomData,
            })),
            cardinality: StatisticsValue::Unknown,
            schema: F::schema(),
        };

        Box::pin(async move { Ok(planned) })
    }
}

#[derive(Debug, Clone)]
struct SystemDataTable<F: SystemFunctionImpl> {
    #[allow(clippy::type_complexity)] // Temp
    databases: Arc<Mutex<Option<VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>>>>,
    _f: PhantomData<F>,
}

impl<F: SystemFunctionImpl> DataTable for SystemDataTable<F> {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        let databases = self
            .databases
            .lock()
            .take()
            .ok_or_else(|| RayexecError::new("Scan called multiple times"))?;

        let mut scans: Vec<Box<dyn DataTableScan>> = vec![Box::new(ProjectedScan::new(
            SystemDataTableScan::<F> {
                databases,
                _f: PhantomData,
            },
            projections,
        )) as _];

        scans.extend((1..num_partitions).map(|_| Box::new(EmptyTableScan) as _));

        Ok(scans)
    }
}

#[derive(Debug)]
struct SystemDataTableScan<F: SystemFunctionImpl> {
    databases: VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    _f: PhantomData<F>,
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