use std::sync::Arc;
use std::task::Context;

use glaredb_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataTypeId;
use crate::arrays::field::ColumnSchema;
use crate::catalog::{Catalog, Schema};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::{ScanContext, TableScanFunction};
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::statistics::value::StatisticsValue;
use crate::storage::datatable::{DataTable, ParallelDataTableScanState};
use crate::storage::projections::Projections;
use crate::storage::scan_filter::PhysicalScanFilter;

pub const FUNCTION_SET_MEMORY_SCAN: TableFunctionSet = TableFunctionSet {
    name: "memory_scan",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::Table,
        description: "Scans a memory table in the database.",
        arguments: &["catalog", "schema", "table"],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(
            &[DataTypeId::Utf8, DataTypeId::Utf8, DataTypeId::Utf8],
            DataTypeId::Table,
        ),
        &MemoryScan,
    )],
};

#[derive(Debug)]
pub struct MemoryScanBindState {
    table: Arc<DataTable>,
}

#[derive(Debug)]
pub struct MemoryScanOperatorState {
    projections: Projections,
    table: Arc<DataTable>,
}

#[derive(Debug)]
pub struct MemoryScanPartitionState {
    state: ParallelDataTableScanState,
}

#[derive(Debug, Clone, Copy)]
pub struct MemoryScan;

impl TableScanFunction for MemoryScan {
    type BindState = MemoryScanBindState;
    type OperatorState = MemoryScanOperatorState;
    type PartitionState = MemoryScanPartitionState;

    async fn bind(
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        // TODO: Avoid the clones.
        // TODO: Avoid all of this? Can we jut pass the entry directly?
        let catalog = input.positional[0]
            .clone()
            .try_into_scalar()?
            .try_into_string()?;
        let schema = input.positional[1]
            .clone()
            .try_into_scalar()?
            .try_into_string()?;
        let table = input.positional[2]
            .clone()
            .try_into_scalar()?
            .try_into_string()?;

        let database = scan_context
            .database_context
            .require_get_database(&catalog)?;

        let ent = database
            .catalog
            .require_get_schema(&schema)?
            .require_get_table(&table)?;

        let ent = ent.try_as_table_entry()?;
        let datatable = database.storage.get_table(ent.storage_id)?;

        Ok(TableFunctionBindState {
            state: MemoryScanBindState { table: datatable },
            input,
            data_schema: ColumnSchema::new(ent.columns.clone()),
            meta_schema: None,
            cardinality: StatisticsValue::Unknown, // TODO
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _filters: &[PhysicalScanFilter],
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(MemoryScanOperatorState {
            projections,
            table: bind_state.table.clone(),
        })
    }

    fn create_pull_partition_states(
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let states = op_state
            .table
            .init_parallel_scan_states(partitions)
            .map(|state| MemoryScanPartitionState { state })
            .collect();

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        _bind_state: &Self::BindState,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let count =
            op_state
                .table
                .parallel_scan(&op_state.projections, &mut state.state, output)?;
        if count == 0 {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}
