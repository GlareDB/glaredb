use std::sync::Arc;
use std::task::Context;

use futures::TryStreamExt;
use glaredb_error::{DbError, Result};

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::array::validity::Validity;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::context::DatabaseContext;
use crate::catalog::database::Database;
use crate::catalog::entry::{CatalogEntry, CatalogEntryType};
use crate::catalog::{Catalog, Schema};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::TableScanFunction;
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;
use crate::storage::projections::Projections;

pub const FUNCTION_SET_LIST_TABLES: TableFunctionSet = TableFunctionSet {
    name: "list_tables",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Lists all tables in the database",
        arguments: &[],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &ListTables,
    )],
};

pub const FUNCTION_SET_LIST_VIEWS: TableFunctionSet = TableFunctionSet {
    name: "list_views",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Lists all views in the database",
        arguments: &[],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &ListViews,
    )],
};

pub const FUNCTION_SET_LIST_FUNCTIONS: TableFunctionSet = TableFunctionSet {
    name: "list_functions",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Lists all functions in the database with their types and metadata",
        arguments: &[],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &ListFunctions,
    )],
};

#[derive(Debug)]
pub struct ListEntriesBindState {
    entries: Arc<Vec<NamespacedEntry>>,
}

impl ListEntriesBindState {
    // TODO: Unsure if we're fine with doing the loading up front, but it's
    // easiest for now. `poll_pull` could handle this, but it'd introduce a bit
    // of code complexity.
    async fn load_entries(
        databases: Vec<Arc<Database>>,
        db_filter: impl Fn(&Database) -> bool,
        schema_filter: impl Fn(&CatalogEntry) -> bool,
        entry_filter: impl Fn(&CatalogEntry) -> bool,
    ) -> Result<Self> {
        let mut filtered_entries = Vec::new();
        for database in databases {
            if !db_filter(&database) {
                continue;
            }

            let db_name: Arc<str> = database.name.clone().into();
            let mut schema_stream = Box::pin(database.catalog.list_schemas());

            while let Some(schemas) = schema_stream.try_next().await? {
                for schema in schemas {
                    if !schema_filter(&schema.as_entry()) {
                        continue;
                    }

                    let schema_name: Arc<str> = schema.as_entry().name.clone().into();
                    let mut entry_stream = Box::pin(schema.list_entries());

                    while let Some(entries) = entry_stream.try_next().await? {
                        let entries = entries.into_iter().filter_map(|ent| {
                            if !entry_filter(&ent) {
                                None
                            } else {
                                Some(NamespacedEntry {
                                    catalog: db_name.clone(),
                                    schema: schema_name.clone(),
                                    ent,
                                })
                            }
                        });

                        filtered_entries.extend(entries);
                    }
                }
            }
        }

        Ok(ListEntriesBindState {
            entries: Arc::new(filtered_entries),
        })
    }
}

#[derive(Debug)]
pub struct ListEntriesOperatorState {
    projections: Projections,
    entries: Arc<Vec<NamespacedEntry>>,
}

#[derive(Debug, Clone, Default)]
pub struct ListEntriesPartitionState {
    offset: usize,
    entries: Vec<NamespacedEntry>,
}

#[derive(Debug, Clone)]
struct NamespacedEntry {
    catalog: Arc<str>,
    schema: Arc<str>,
    ent: Arc<CatalogEntry>,
}

#[derive(Debug, Clone, Copy)]
pub struct ListTables;

impl TableScanFunction for ListTables {
    type BindState = ListEntriesBindState;
    type OperatorState = ListEntriesOperatorState;
    type PartitionState = ListEntriesPartitionState;

    async fn bind(
        &'static self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let databases: Vec<_> = db_context.iter_databases().cloned().collect();
        let state = ListEntriesBindState::load_entries(
            databases,
            |_| true,
            |_| true,
            |ent| ent.entry_type() == CatalogEntryType::Table,
        )
        .await?;

        Ok(TableFunctionBindState {
            state,
            input,
            schema: ColumnSchema::new([
                Field::new("database_name", DataType::Utf8, false),
                Field::new("schema_name", DataType::Utf8, false),
                Field::new("table_name", DataType::Utf8, false),
            ]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListEntriesOperatorState {
            projections: projections.clone(),
            entries: bind_state.entries.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut states = vec![ListEntriesPartitionState::default(); partitions];

        for (idx, chunk) in op_state.entries.chunks(props.batch_size).enumerate() {
            let part_idx = idx % states.len();
            states[part_idx].entries.extend(chunk.iter().cloned());
        }

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let cap = output.write_capacity()?;
        let count = usize::min(cap, state.entries.len() - state.offset);

        op_state
            .projections
            .for_each_column(output, &mut |col_idx, output| match col_idx {
                0 => {
                    // Catalog name
                    let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        db_names.put(idx, &state.entries[idx + state.offset].catalog);
                    }
                    Ok(())
                }
                1 => {
                    // Schema name
                    let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        schema_names.put(idx, &state.entries[idx + state.offset].schema);
                    }
                    Ok(())
                }
                2 => {
                    // Table name
                    let mut table_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        table_names.put(idx, &state.entries[idx + state.offset].ent.name);
                    }
                    Ok(())
                }
                other => panic!("unexpected projection: {other}"),
            })?;

        output.set_num_rows(count)?;
        state.offset += count;

        if state.entries.len() - state.offset == 0 {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListViews;

impl TableScanFunction for ListViews {
    type BindState = ListEntriesBindState;
    type OperatorState = ListEntriesOperatorState;
    type PartitionState = ListEntriesPartitionState;

    async fn bind(
        &'static self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let databases: Vec<_> = db_context.iter_databases().cloned().collect();
        let state = ListEntriesBindState::load_entries(
            databases,
            |_| true,
            |_| true,
            |ent| ent.entry_type() == CatalogEntryType::View,
        )
        .await?;

        Ok(TableFunctionBindState {
            state,
            input,
            schema: ColumnSchema::new([
                Field::new("database_name", DataType::Utf8, false),
                Field::new("schema_name", DataType::Utf8, false),
                Field::new("view_name", DataType::Utf8, false),
            ]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListEntriesOperatorState {
            projections: projections.clone(),
            entries: bind_state.entries.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut states = vec![ListEntriesPartitionState::default(); partitions];

        for (idx, chunk) in op_state.entries.chunks(props.batch_size).enumerate() {
            let part_idx = idx % states.len();
            states[part_idx].entries.extend(chunk.iter().cloned());
        }

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let cap = output.write_capacity()?;
        let count = usize::min(cap, state.entries.len() - state.offset);

        op_state
            .projections
            .for_each_column(output, &mut |col_idx, output| match col_idx {
                0 => {
                    // Catalog name
                    let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        db_names.put(idx, &state.entries[idx + state.offset].catalog);
                    }
                    Ok(())
                }
                1 => {
                    // Schema name
                    let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        schema_names.put(idx, &state.entries[idx + state.offset].schema);
                    }
                    Ok(())
                }
                2 => {
                    // View name
                    let mut table_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        table_names.put(idx, &state.entries[idx + state.offset].ent.name);
                    }
                    Ok(())
                }
                other => panic!("unexpected projection: {other}"),
            })?;

        output.set_num_rows(count)?;
        state.offset += count;

        if state.entries.len() - state.offset == 0 {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ListFunctions;

impl ListFunctions {
    fn try_function_type_as_str(ent: &CatalogEntry) -> Result<&'static str> {
        Ok(match ent.entry_type() {
            CatalogEntryType::ScalarFunction => "scalar",
            CatalogEntryType::AggregateFunction => "aggregate",
            CatalogEntryType::TableFunction => "table",
            CatalogEntryType::CopyToFunction => "copy",
            other => return Err(DbError::new(format!("Unexpected entry type: {other}"))),
        })
    }
}

impl TableScanFunction for ListFunctions {
    type BindState = ListEntriesBindState;
    type OperatorState = ListEntriesOperatorState;
    type PartitionState = ListEntriesPartitionState;

    async fn bind(
        &'static self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let databases: Vec<_> = db_context.iter_databases().cloned().collect();
        // TODO: Do we want COPY functions in the output?
        let state = ListEntriesBindState::load_entries(
            databases,
            |_| true,
            |_| true,
            |ent| {
                matches!(
                    ent.entry_type(),
                    CatalogEntryType::TableFunction
                        | CatalogEntryType::ScalarFunction
                        | CatalogEntryType::AggregateFunction
                        | CatalogEntryType::CopyToFunction
                )
            },
        )
        .await?;

        Ok(TableFunctionBindState {
            state,
            input,
            schema: ColumnSchema::new([
                Field::new("database_name", DataType::Utf8, false),
                Field::new("schema_name", DataType::Utf8, false),
                Field::new("function_name", DataType::Utf8, false),
                Field::new("function_type", DataType::Utf8, false),
                Field::new(
                    "argument_types",
                    DataType::List(ListTypeMeta::new(DataType::Utf8)),
                    false,
                ),
                Field::new("return_type", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, false),
                Field::new("example", DataType::Utf8, false),
                Field::new("example_output", DataType::Utf8, false),
            ]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListEntriesOperatorState {
            projections: projections.clone(),
            entries: bind_state.entries.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut states = vec![ListEntriesPartitionState::default(); partitions];

        for (idx, chunk) in op_state.entries.chunks(props.batch_size).enumerate() {
            let part_idx = idx % states.len();
            states[part_idx].entries.extend(chunk.iter().cloned());
        }

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let cap = output.write_capacity()?;
        let count = usize::min(cap, state.entries.len() - state.offset);

        // TODO: Need a row for each function arity.

        op_state
            .projections
            .for_each_column(output, &mut |col_idx, output| match col_idx {
                0 => {
                    // Catalog name
                    let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        db_names.put(idx, &state.entries[idx + state.offset].catalog);
                    }
                    Ok(())
                }
                1 => {
                    // Schema name
                    let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        schema_names.put(idx, &state.entries[idx + state.offset].schema);
                    }
                    Ok(())
                }
                2 => {
                    // Function name
                    let mut function_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        function_names.put(idx, &state.entries[idx + state.offset].ent.name);
                    }
                    Ok(())
                }
                3 => {
                    // Function type
                    let mut function_types = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        let typ =
                            Self::try_function_type_as_str(&state.entries[idx + state.offset].ent)?;
                        function_types.put(idx, typ);
                    }
                    Ok(())
                }
                4 => {
                    // Argument types
                    // TODO
                    let validity = Validity::new_all_invalid(output.logical_len());
                    output.put_validity(validity)?;
                    Ok(())
                }
                5 => {
                    // Return type
                    // TODO
                    let validity = Validity::new_all_invalid(output.logical_len());
                    output.put_validity(validity)?;
                    Ok(())
                }
                6 => {
                    // Description
                    // TODO
                    let validity = Validity::new_all_invalid(output.logical_len());
                    output.put_validity(validity)?;
                    Ok(())
                }
                7 => {
                    // Example
                    // TODO
                    let validity = Validity::new_all_invalid(output.logical_len());
                    output.put_validity(validity)?;
                    Ok(())
                }
                8 => {
                    // Example output
                    // TODO
                    let validity = Validity::new_all_invalid(output.logical_len());
                    output.put_validity(validity)?;
                    Ok(())
                }
                other => panic!("unexpected projection: {other}"),
            })?;

        output.set_num_rows(count)?;
        state.offset += count;

        if state.entries.len() - state.offset == 0 {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}
