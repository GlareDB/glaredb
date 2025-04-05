use std::sync::Arc;
use std::task::Context;

use glaredb_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::context::DatabaseContext;
use crate::catalog::database::Database;
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::TableScanFunction;
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;
use crate::storage::projections::{ProjectedColumn, Projections};

pub const FUNCTION_SET_LIST_DATABASES: TableFunctionSet = TableFunctionSet {
    name: "list_databases",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Lists databases this session has access to.",
        arguments: &[],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &ListDatabases,
    )],
};

#[derive(Debug)]
pub struct ListDatabasesBindState {
    databases: Vec<Arc<Database>>,
}

#[derive(Debug)]
pub struct ListDatabasesOperatorState {
    projections: Projections,
    databases: Vec<Arc<Database>>,
}

#[derive(Debug)]
pub struct ListDatabasePartitionState {
    offset: usize,
    databases: Vec<Arc<Database>>,
}

#[derive(Debug, Clone, Copy)]
pub struct ListDatabases;

impl TableScanFunction for ListDatabases {
    type BindState = ListDatabasesBindState;
    type OperatorState = ListDatabasesOperatorState;
    type PartitionState = ListDatabasePartitionState;

    async fn bind(
        &'static self,
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let databases = db_context.iter_databases().cloned().collect();
        Ok(TableFunctionBindState {
            state: ListDatabasesBindState { databases },
            input,
            schema: ColumnSchema::new([
                Field::new("database_name", DataType::Utf8, false),
                Field::new("access_mode", DataType::Utf8, false),
            ]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListDatabasesOperatorState {
            projections: projections.clone(),
            databases: bind_state.databases.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut states = vec![ListDatabasePartitionState {
            offset: 0,
            databases: op_state.databases.clone(),
        }];
        states.resize_with(partitions, || ListDatabasePartitionState {
            offset: 0,
            databases: Vec::new(),
        });

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let cap = output.write_capacity()?;
        let count = usize::min(cap, state.databases.len() - state.offset);

        op_state
            .projections
            .for_each_column(output, &mut |col_idx, output| match col_idx {
                ProjectedColumn::Data(0) => {
                    let mut names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        names.put(idx, &state.databases[idx + state.offset].name);
                    }
                    Ok(())
                }
                ProjectedColumn::Data(1) => {
                    let mut access_modes = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                    for idx in 0..count {
                        access_modes.put(idx, state.databases[idx + state.offset].mode.as_str());
                    }
                    Ok(())
                }
                other => panic!("unexpected projection: {other:?}"),
            })?;

        output.set_num_rows(count)?;
        state.offset += count;

        if count == cap {
            Ok(PollPull::HasMore)
        } else {
            Ok(PollPull::Exhausted)
        }
    }
}
