use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::future::BoxFuture;
use parking_lot::Mutex;
use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::bitmap::Bitmap;
use rayexec_bullet::datatype::{DataTypeId, DataTypeOld, ListTypeMeta};
use rayexec_bullet::executor::builder::{ArrayDataBuffer, GermanVarlenBuffer};
use rayexec_bullet::field::{Field, Schema};
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_bullet::storage::{GermanVarlenStorage, ListItemMetadata, ListStorage};
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
    ) -> Result<BatchOld>;
}

pub type ListDatabases = SystemFunction<ListDatabasesImpl>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListDatabasesImpl;

impl SystemFunctionImpl for ListDatabasesImpl {
    const NAME: &'static str = "list_databases";

    fn schema() -> Schema {
        Schema::new([
            Field::new("database_name", DataTypeOld::Utf8, false),
            Field::new("datasource", DataTypeOld::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<BatchOld> {
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

        BatchOld::try_new([
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, database_names.into_data()),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, datasources.into_data()),
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
            Field::new("database_name", DataTypeOld::Utf8, false),
            Field::new("schema_name", DataTypeOld::Utf8, false),
            Field::new("function_name", DataTypeOld::Utf8, false),
            Field::new("function_type", DataTypeOld::Utf8, false),
            Field::new(
                "argument_types",
                DataTypeOld::List(ListTypeMeta::new(DataTypeOld::Utf8)),
                false,
            ),
            Field::new(
                "argument_names",
                DataTypeOld::List(ListTypeMeta::new(DataTypeOld::Utf8)),
                false,
            ),
            Field::new("return_type", DataTypeOld::Utf8, false),
            Field::new("description", DataTypeOld::Utf8, true),
            Field::new("example", DataTypeOld::Utf8, true),
            Field::new("example_output", DataTypeOld::Utf8, true),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<BatchOld> {
        let database = databases.pop_front().required("database")?;

        let mut database_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut schema_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut function_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut function_types = GermanVarlenStorage::with_metadata_capacity(0);

        // TODO: List ergonomics.
        let mut argument_types_metadatas = Vec::new();
        let mut argument_types = GermanVarlenStorage::with_metadata_capacity(0);

        let mut argument_names_metadatas = Vec::new();
        let mut argument_names = GermanVarlenStorage::with_metadata_capacity(0);

        let mut return_types = GermanVarlenStorage::with_metadata_capacity(0);

        let mut descriptions_validity = Bitmap::default();
        let mut descriptions = GermanVarlenStorage::with_metadata_capacity(0);

        let mut examples_validity = Bitmap::default();
        let mut examples = GermanVarlenStorage::with_metadata_capacity(0);

        let mut example_outputs_validity = Bitmap::default();
        let mut example_outputs = GermanVarlenStorage::with_metadata_capacity(0);

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, schema| {
            schema.for_each_entry(tx, &mut |_, entry| {
                let (sigs, function_type) = match &entry.entry {
                    CatalogEntryInner::ScalarFunction(func) => {
                        (func.function.signatures(), "scalar")
                    }
                    CatalogEntryInner::AggregateFunction(func) => {
                        (func.function.signatures(), "aggregate")
                    }
                    CatalogEntryInner::TableFunction(func) => (func.function.signatures(), "table"),
                    _ => return Ok(()),
                };

                for sig in sigs {
                    database_names.try_push(database.0.as_bytes())?;
                    schema_names.try_push(schema_name.as_bytes())?;
                    function_names.try_push(entry.name.as_bytes())?;
                    function_types.try_push(function_type.as_bytes())?;

                    argument_types_metadatas.push(ListItemMetadata {
                        offset: argument_types.len() as i32,
                        len: sig.positional_args.len() as i32,
                    });
                    for arg in sig.positional_args {
                        argument_types.try_push(arg.as_str().as_bytes())?;
                    }

                    argument_names_metadatas.push(ListItemMetadata {
                        offset: argument_names.len() as i32,
                        len: sig.positional_args.len() as i32,
                    });
                    match sig.doc {
                        Some(doc) => {
                            for idx in 0..sig.positional_args.len() {
                                match doc.arguments.get(idx) {
                                    Some(name) => argument_names.try_push(name.as_bytes())?,
                                    None => {
                                        argument_names.try_push(format!("col{idx}").as_bytes())?
                                    }
                                }
                            }
                        }
                        None => {
                            for idx in 0..sig.positional_args.len() {
                                argument_names.try_push(format!("col{idx}").as_bytes())?;
                            }
                        }
                    }

                    return_types.try_push(sig.return_type.as_str().as_bytes())?;

                    match sig.doc {
                        Some(doc) => {
                            descriptions_validity.push(true);
                            descriptions.try_push(doc.description.as_bytes())?;
                        }
                        None => {
                            descriptions_validity.push(false);
                            descriptions.try_push(&[])?;
                        }
                    }

                    match sig.doc.and_then(|doc| doc.example.as_ref()) {
                        Some(example) => {
                            examples_validity.push(true);
                            examples.try_push(example.example.as_bytes())?;

                            example_outputs_validity.push(true);
                            example_outputs.try_push(example.output.as_bytes())?;
                        }
                        None => {
                            examples_validity.push(false);
                            examples.try_push(&[])?;

                            example_outputs_validity.push(false);
                            example_outputs.try_push(&[])?;
                        }
                    }
                }

                Ok(())
            })?;
            Ok(())
        })?;

        BatchOld::try_new([
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, database_names),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, schema_names),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, function_names),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, function_types),
            ArrayOld::new_with_array_data(
                DataTypeOld::List(ListTypeMeta::new(DataTypeOld::Utf8)),
                ListStorage::try_new(
                    argument_types_metadatas,
                    ArrayOld::new_with_array_data(DataTypeOld::Utf8, argument_types),
                )?,
            ),
            ArrayOld::new_with_array_data(
                DataTypeOld::List(ListTypeMeta::new(DataTypeOld::Utf8)),
                ListStorage::try_new(
                    argument_names_metadatas,
                    ArrayOld::new_with_array_data(DataTypeOld::Utf8, argument_names),
                )?,
            ),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, return_types),
            ArrayOld::new_with_validity_and_array_data(
                DataTypeOld::Utf8,
                descriptions_validity,
                descriptions,
            ),
            ArrayOld::new_with_validity_and_array_data(
                DataTypeOld::Utf8,
                examples_validity,
                examples,
            ),
            ArrayOld::new_with_validity_and_array_data(
                DataTypeOld::Utf8,
                example_outputs_validity,
                example_outputs,
            ),
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
            Field::new("database_name", DataTypeOld::Utf8, false),
            Field::new("schema_name", DataTypeOld::Utf8, false),
            Field::new("table_name", DataTypeOld::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<BatchOld> {
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

        BatchOld::try_new([
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, database_names),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, schema_names),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, table_names),
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
            Field::new("database_name", DataTypeOld::Utf8, false),
            Field::new("schema_name", DataTypeOld::Utf8, false),
        ])
    }

    fn new_batch(
        databases: &mut VecDeque<(String, Arc<MemoryCatalog>, Option<AttachInfo>)>,
    ) -> Result<BatchOld> {
        let database = databases.pop_front().required("database")?;

        let mut database_names = GermanVarlenStorage::with_metadata_capacity(0);
        let mut schema_names = GermanVarlenStorage::with_metadata_capacity(0);

        let tx = &CatalogTx {};

        database.1.for_each_schema(tx, &mut |schema_name, _| {
            database_names.try_push(database.0.as_bytes())?;
            schema_names.try_push(schema_name.as_bytes())?;

            Ok(())
        })?;

        BatchOld::try_new([
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, database_names),
            ArrayOld::new_with_array_data(DataTypeOld::Utf8, schema_names),
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
    fn pull(&mut self) -> BoxFuture<'_, Result<Option<BatchOld>>> {
        Box::pin(async {
            if self.databases.is_empty() {
                return Ok(None);
            }

            let batch = F::new_batch(&mut self.databases)?;

            Ok(Some(batch))
        })
    }
}
