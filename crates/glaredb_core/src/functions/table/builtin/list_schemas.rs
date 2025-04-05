use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use glaredb_error::Result;

use crate::arrays::array::physical_type::{AddressableMut, MutableScalarStorage, PhysicalUtf8};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::arrays::field::{ColumnSchema, Field};
use crate::catalog::database::Database;
use crate::catalog::memory::MemorySchema;
use crate::catalog::{Catalog, Schema};
use crate::execution::operators::{ExecutionProperties, PollPull};
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation};
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::scan::{ScanContext, TableScanFunction};
use crate::functions::table::{RawTableFunction, TableFunctionBindState, TableFunctionInput};
use crate::logical::statistics::StatisticsValue;
use crate::storage::projections::{ProjectedColumn, Projections};

pub const FUNCTION_SET_LIST_SCHEMAS: TableFunctionSet = TableFunctionSet {
    name: "list_schemas",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::System,
        description: "Lists all schemas this session has access to.",
        arguments: &[],
        example: None,
    }],
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &ListSchemas,
    )],
};

pub struct ListSchemasBindState {
    databases: Vec<Arc<Database>>,
}

pub struct ListSchemasOperatorState {
    projections: Projections,
    databases: Vec<Arc<Database>>,
}

// TODO: Make simpler.
type SchemaStream = Pin<Box<dyn Stream<Item = Result<Vec<Arc<MemorySchema>>>> + Sync + Send>>;

pub struct ListSchemasPartitionState {
    db_offset: usize,
    databases: Vec<Arc<Database>>,
    curr_stream: Option<SchemaStream>,
    schemas_offset: usize,
    schemas: Vec<Arc<MemorySchema>>,
}

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;

impl TableScanFunction for ListSchemas {
    type BindState = ListSchemasBindState;
    type OperatorState = ListSchemasOperatorState;
    type PartitionState = ListSchemasPartitionState;

    async fn bind(
        &'static self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<TableFunctionBindState<Self::BindState>> {
        let databases = scan_context
            .database_context
            .iter_databases()
            .cloned()
            .collect();
        Ok(TableFunctionBindState {
            state: ListSchemasBindState { databases },
            input,
            schema: ColumnSchema::new([
                Field::new("database_name", DataType::Utf8, false),
                Field::new("schema_name", DataType::Utf8, false),
            ]),
            cardinality: StatisticsValue::Unknown,
        })
    }

    fn create_pull_operator_state(
        bind_state: &Self::BindState,
        projections: Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(ListSchemasOperatorState {
            projections,
            databases: bind_state.databases.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        _props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let mut states = vec![ListSchemasPartitionState {
            db_offset: 0,
            databases: op_state.databases.clone(),
            curr_stream: None,
            schemas_offset: 0,
            schemas: Vec::new(),
        }];

        states.resize_with(partitions, || ListSchemasPartitionState {
            db_offset: 0,
            databases: Vec::new(),
            curr_stream: None,
            schemas_offset: 0,
            schemas: Vec::new(),
        });

        Ok(states)
    }

    fn poll_pull(
        cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        if state.schemas_offset >= state.schemas.len() {
            if state.curr_stream.is_none() {
                if state.db_offset >= state.databases.len() {
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }

                let stream = state.databases[state.db_offset].catalog.list_schemas();
                state.curr_stream = Some(Box::pin(stream));
            }

            // Stream should be Some here.
            loop {
                let stream = state.curr_stream.as_mut().unwrap();
                match stream.poll_next_unpin(cx) {
                    Poll::Ready(Some(result)) => {
                        let schemas = result?;
                        if !schemas.is_empty() {
                            state.schemas_offset = 0;
                            state.schemas = schemas;
                            break;
                        }
                        // Schemas is empty, keep pull from the stream.
                    }
                    Poll::Ready(None) => {
                        // Yield, and come back to get the next db. Mostly doing
                        // this to avoid nested loops here.
                        state.db_offset += 1;
                        state.curr_stream = None;
                        output.set_num_rows(0)?;
                        return Ok(PollPull::HasMore);
                    }
                    Poll::Pending => {
                        return Ok(PollPull::Pending);
                    }
                }
            }
        }

        let cap = output.write_capacity()?;
        let count = usize::min(cap, state.schemas.len() - state.schemas_offset);

        // Schemas should be non-empty.
        op_state
            .projections
            .for_each_column(output, &mut |col_idx, output| {
                match col_idx {
                    ProjectedColumn::Data(0) => {
                        let mut db_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                        for idx in 0..count {
                            // Note we're getting the same db every time.
                            db_names.put(idx, &state.databases[state.db_offset].name);
                        }
                        Ok(())
                    }
                    ProjectedColumn::Data(1) => {
                        let mut schema_names = PhysicalUtf8::get_addressable_mut(&mut output.data)?;
                        for idx in 0..count {
                            schema_names.put(
                                idx,
                                &state.schemas[idx + state.schemas_offset].as_entry().name,
                            );
                        }
                        Ok(())
                    }
                    other => panic!("unexpected projection: {other:?}"),
                }
            })?;

        output.set_num_rows(count)?;
        state.schemas_offset += count;

        Ok(PollPull::HasMore)
    }
}
