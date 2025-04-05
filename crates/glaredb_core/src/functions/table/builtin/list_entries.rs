use std::borrow::Cow;
use std::fmt::Write;
use std::sync::Arc;
use std::task::Context;

use futures::TryStreamExt;
use glaredb_error::{DbError, Result};

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::arrays::field::{ColumnSchema, Field};
use crate::arrays::scalar::ScalarValue;
use crate::catalog::context::DatabaseContext;
use crate::catalog::database::Database;
use crate::catalog::entry::{CatalogEntry, CatalogEntryInner, CatalogEntryType};
use crate::catalog::{Catalog, Schema};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::TableScanFunction;
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;
use crate::storage::projections::{ProjectedColumn, Projections};

pub const FUNCTION_SET_LIST_TABLES: TableFunctionSet = TableFunctionSet {
    name: "list_tables",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Lists all tables this session has access to.",
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
        description: "Lists all views this session has access to.",
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
        description: "Lists all functions in the database with their types and metadata.",
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
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListEntriesOperatorState {
            projections,
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
                ProjectedColumn::Data(0) => {
                    // Catalog name
                    let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        db_names.put(idx, &state.entries[idx + state.offset].catalog);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    // Schema name
                    let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        schema_names.put(idx, &state.entries[idx + state.offset].schema);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    // Table name
                    let mut table_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        table_names.put(idx, &state.entries[idx + state.offset].ent.name);
                    }
                    Ok(())
                }
                other => panic!("unexpected projection: {other:?}"),
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
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListEntriesOperatorState {
            projections,
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
                ProjectedColumn::Data(0) => {
                    // Catalog name
                    let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        db_names.put(idx, &state.entries[idx + state.offset].catalog);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    // Schema name
                    let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        schema_names.put(idx, &state.entries[idx + state.offset].schema);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    // View name
                    let mut table_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        table_names.put(idx, &state.entries[idx + state.offset].ent.name);
                    }
                    Ok(())
                }
                other => panic!("unexpected projection: {other:?}"),
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

#[derive(Debug, Clone, Default)]
pub struct ListFunctionsPartitionState {
    offset: usize,
    entries: Vec<NamespacedFunction>,
}

#[derive(Debug, Clone)]
struct NamespacedFunction {
    entry: NamespacedEntry,
    signature: &'static Signature,
    doc: Option<&'static Documentation>,
}

/// Get the documentation object that matches the provided arity.
///
/// This is best effort, we might have a cleaner way of lining up which doc goes
/// to which sig in the future.
fn get_doc_for_arity(
    docs: &'static [&'static Documentation],
    arity: usize,
) -> Option<&'static Documentation> {
    docs.iter()
        .find(|&doc| doc.arguments.len() == arity)
        .copied()
}

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
    type PartitionState = ListFunctionsPartitionState;

    async fn bind(
        &'static self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let databases: Vec<_> = db_context.iter_databases().cloned().collect();
        // TODO: Do we want COPY functions in the output? Not yet
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
                Field::new("category", DataType::Utf8, false),
                Field::new(
                    "arguments",
                    DataType::List(ListTypeMeta::new(DataType::Utf8)),
                    false,
                ),
                Field::new(
                    "argument_types",
                    DataType::List(ListTypeMeta::new(DataType::Utf8)),
                    false,
                ),
                Field::new("return_type", DataType::Utf8, false),
                Field::new("description", DataType::Utf8, true),
                Field::new("example", DataType::Utf8, true),
                Field::new("example_output", DataType::Utf8, true),
            ]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListEntriesOperatorState {
            projections,
            entries: bind_state.entries.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut states = vec![ListFunctionsPartitionState::default(); partitions];

        for (idx, chunk) in op_state.entries.chunks(props.batch_size).enumerate() {
            let part_idx = idx % states.len();

            // TODO: Do I care about perf here?
            for entry in chunk {
                // entry entry entry
                match &entry.ent.entry {
                    CatalogEntryInner::ScalarFunction(f) => {
                        for sig in f.function.functions.iter().map(|f| f.signature()) {
                            states[part_idx].entries.push(NamespacedFunction {
                                entry: entry.clone(),
                                signature: sig,
                                doc: get_doc_for_arity(f.function.doc, sig.positional_args.len()),
                            });
                        }
                    }
                    CatalogEntryInner::AggregateFunction(f) => {
                        for sig in f.function.functions.iter().map(|f| f.signature()) {
                            states[part_idx].entries.push(NamespacedFunction {
                                entry: entry.clone(),
                                signature: sig,
                                doc: get_doc_for_arity(f.function.doc, sig.positional_args.len()),
                            });
                        }
                    }
                    CatalogEntryInner::TableFunction(f) => {
                        for sig in f.function.functions.iter().map(|f| f.signature()) {
                            states[part_idx].entries.push(NamespacedFunction {
                                entry: entry.clone(),
                                signature: sig,
                                doc: get_doc_for_arity(f.function.doc, sig.positional_args.len()),
                            });
                        }
                    }
                    _ => return Err(DbError::new("Unexpected catalog entry type")),
                }
            }
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
                ProjectedColumn::Data(0) => {
                    // Catalog name
                    let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        db_names.put(idx, &state.entries[idx + state.offset].entry.catalog);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    // Schema name
                    let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        schema_names.put(idx, &state.entries[idx + state.offset].entry.schema);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(2) => {
                    // Function name
                    let mut function_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        function_names.put(idx, &state.entries[idx + state.offset].entry.ent.name);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(3) => {
                    // Function type
                    let mut function_types = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        let typ = Self::try_function_type_as_str(
                            &state.entries[idx + state.offset].entry.ent,
                        )?;
                        function_types.put(idx, typ);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(4) => {
                    // Category
                    let (data, validity) = output.data_and_validity_mut();
                    let mut categories = PhysicalUtf8::get_addressable_mut(data)?;
                    for idx in 0..count {
                        match state.entries[idx + state.offset].doc.as_ref() {
                            Some(doc) => categories.put(idx, doc.category.as_str()),
                            None => validity.set_invalid(idx),
                        }
                    }
                    Ok(())
                }
                ProjectedColumn::Data(5) => {
                    // Arguments
                    for idx in 0..count {
                        let ent = &state.entries[idx + state.offset];
                        // TODO: No need to allocate every time.
                        let args = match ent.doc {
                            Some(doc) => doc
                                .arguments
                                .iter()
                                .map(|&arg| ScalarValue::Utf8(Cow::Borrowed(arg)))
                                .collect(),
                            None => {
                                // We don't have documentation for this function
                                // arity, just use const names.
                                vec![
                                    ScalarValue::Utf8("col".into());
                                    ent.signature.positional_args.len()
                                ]
                            }
                        };

                        // TODO: Not sure I care about efficiency here.
                        output.set_value(idx, &ScalarValue::List(args))?;
                    }

                    Ok(())
                }
                ProjectedColumn::Data(6) => {
                    // Argument types
                    for idx in 0..count {
                        let mut types = Vec::new(); // TODO: No need to allocate every time.

                        types.extend(
                            state.entries[idx + state.offset]
                                .signature
                                .positional_args
                                .iter()
                                .map(|arg_type| ScalarValue::Utf8(arg_type.to_string().into())),
                        );

                        // TODO: Not sure I care about efficiency here.
                        output.set_value(idx, &ScalarValue::List(types))?;
                    }

                    Ok(())
                }
                ProjectedColumn::Data(7) => {
                    // Return type
                    let mut ret_types = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    let mut s_buf = String::new();
                    for idx in 0..count {
                        s_buf.clear();
                        write!(
                            s_buf,
                            "{}",
                            state.entries[idx + state.offset].signature.return_type
                        )?;
                        ret_types.put(idx, &s_buf);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(8) => {
                    // Description
                    let (data, validity) = output.data_and_validity_mut();
                    let mut descriptions = PhysicalUtf8::get_addressable_mut(data)?;
                    for idx in 0..count {
                        match state.entries[idx + state.offset].doc {
                            Some(doc) => descriptions.put(idx, doc.description),
                            None => validity.set_invalid(idx),
                        }
                    }
                    Ok(())
                }
                ProjectedColumn::Data(9) => {
                    // Example
                    let (data, validity) = output.data_and_validity_mut();
                    let mut examples = PhysicalUtf8::get_addressable_mut(data)?;
                    for idx in 0..count {
                        match state.entries[idx + state.offset]
                            .doc
                            .and_then(|d| d.example.as_ref())
                        {
                            Some(example) => examples.put(idx, example.example),
                            None => validity.set_invalid(idx),
                        }
                    }
                    Ok(())
                }
                ProjectedColumn::Data(10) => {
                    // Example output
                    let (data, validity) = output.data_and_validity_mut();
                    let mut example_outputs = PhysicalUtf8::get_addressable_mut(data)?;
                    for idx in 0..count {
                        match state.entries[idx + state.offset]
                            .doc
                            .and_then(|d| d.example.as_ref())
                        {
                            Some(example) => example_outputs.put(idx, example.output),
                            None => validity.set_invalid(idx),
                        }
                    }
                    Ok(())
                }
                other => panic!("unexpected projection: {other:?}"),
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
