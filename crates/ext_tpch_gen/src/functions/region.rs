use std::task::Context;

use glaredb_error::Result;
use glaredb_execution::arrays::array::physical_type::{
    AddressableMut,
    MutableScalarStorage,
    PhysicalI32,
    PhysicalUtf8,
};
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::arrays::datatype::{DataType, DataTypeId};
use glaredb_execution::arrays::field::{ColumnSchema, Field};
use glaredb_execution::catalog::context::DatabaseContext;
use glaredb_execution::execution::operators::{ExecutionProperties, PollPull};
use glaredb_execution::functions::function_set::TableFunctionSet;
use glaredb_execution::functions::table::scan::TableScanFunction;
use glaredb_execution::functions::table::{
    RawTableFunction,
    TableFunctionBindState,
    TableFunctionInput,
};
use glaredb_execution::functions::Signature;
use glaredb_execution::logical::statistics::StatisticsValue;
use glaredb_execution::storage::projections::Projections;
use tpchgen::generators::{Region, RegionGenerator, RegionGeneratorIterator};

pub const FUNCTION_SET_REGION: TableFunctionSet = TableFunctionSet {
    name: "region",
    aliases: &[],
    doc: None,
    functions: &[RawTableFunction::new_scan(
        &Signature::new(&[], DataTypeId::Table),
        &RegionFunc,
    )],
};

pub struct RegionBindState {}

pub struct RegionOperatorState {
    projections: Projections,
}

pub struct RegionPartitionState {
    iter: Option<RegionGeneratorIterator<'static>>,
    buffer: Vec<Region<'static>>,
}

#[derive(Debug, Clone, Copy)]
pub struct RegionFunc;

impl TableScanFunction for RegionFunc {
    type BindState = RegionBindState;
    type OperatorState = RegionOperatorState;
    type PartitionState = RegionPartitionState;

    fn bind<'a>(
        &self,
        _db_context: &'a DatabaseContext,
        input: TableFunctionInput,
    ) -> impl Future<Output = Result<TableFunctionBindState<Self::BindState>>> + Sync + Send + 'a
    {
        async {
            Ok(TableFunctionBindState {
                state: RegionBindState {},
                input,
                schema: ColumnSchema::new([
                    Field::new("r_regionkey", DataType::Int32, false),
                    Field::new("r_name", DataType::Utf8, false),
                    Field::new("r_comment", DataType::Utf8, false),
                ]),
                cardinality: StatisticsValue::Unknown,
            })
        }
    }

    fn create_pull_operator_state(
        _bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(RegionOperatorState {
            projections: projections.clone(),
        })
    }

    fn create_pull_partition_states(
        _op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        debug_assert!(partitions >= 1);

        // Single threaded for now, one partition generates, all others just
        // immediately exhuast.
        let mut states = vec![RegionPartitionState {
            iter: Some(RegionGenerator::new().iter()),
            buffer: Vec::with_capacity(props.batch_size),
        }];
        states.resize_with(partitions, || RegionPartitionState {
            iter: None,
            buffer: Vec::new(),
        });

        Ok(states)
    }

    fn poll_pull(
        _cx: &mut Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        let iter = match state.iter.as_mut() {
            Some(iter) => iter,
            None => {
                // This partition isn't generating anything.
                output.set_num_rows(0)?;
                return Ok(PollPull::Exhausted);
            }
        };

        let cap = output.write_capacity()?;

        // Generate the next batch of regions.
        state.buffer.clear();
        state.buffer.extend(iter.take(cap));

        op_state
            .projections
            .for_each_column(output, &mut |col_idx, output| match col_idx {
                0 => {
                    let mut r_keys = PhysicalI32::get_addressable_mut(output.data_mut())?;
                    for (idx, region) in state.buffer.iter().enumerate() {
                        r_keys.put(idx, &(region.r_regionkey as i32));
                    }
                    Ok(())
                }
                1 => {
                    let mut r_names = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                    for (idx, region) in state.buffer.iter().enumerate() {
                        r_names.put(idx, region.r_name);
                    }
                    Ok(())
                }
                2 => {
                    let mut r_comments = PhysicalUtf8::get_addressable_mut(output.data_mut())?;
                    for (idx, region) in state.buffer.iter().enumerate() {
                        r_comments.put(idx, region.r_comment);
                    }
                    Ok(())
                }
                other => panic!("invalid projection {other}"),
            })?;

        let count = state.buffer.len();
        output.set_num_rows(count)?;

        if count < cap {
            Ok(PollPull::Exhausted)
        } else {
            Ok(PollPull::HasMore)
        }
    }
}
