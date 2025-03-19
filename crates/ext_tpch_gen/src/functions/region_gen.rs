use glaredb_error::{DbError, Result};
use glaredb_execution::arrays::batch::Batch;
use glaredb_execution::catalog::context::DatabaseContext;
use glaredb_execution::execution::operators::{ExecutionProperties, PollPull};
use glaredb_execution::functions::table::scan::TableScanFunction;
use glaredb_execution::functions::table::{TableFunctionBindState, TableFunctionInput};
use glaredb_execution::storage::projections::Projections;
use tpchgen::generators::{RegionGenerator, RegionGeneratorIterator};

pub struct RegionGenOperatorState {
    projections: Projections,
}

pub struct RegionGenPartitionState {
    inner: Option<Inner>,
}

struct Inner {
    generator: RegionGenerator<'static>,
    iter: RegionGeneratorIterator<'static>,
}

#[derive(Debug, Clone, Copy)]
pub struct RegionGen;

impl TableScanFunction for RegionGen {
    type BindState = ();
    type OperatorState = RegionGenOperatorState;
    type PartitionState = RegionGenPartitionState;

    fn bind<'a>(
        &self,
        db_context: &'a DatabaseContext,
        input: TableFunctionInput,
    ) -> impl Future<Output = Result<TableFunctionBindState<Self::BindState>>> + Sync + Send + 'a
    {
        async { Err(DbError::new("todo")) }
    }

    fn create_pull_operator_state(
        _bind_state: &Self::BindState,
        projections: &Projections,
        _props: ExecutionProperties,
    ) -> Result<Self::OperatorState> {
        Ok(RegionGenOperatorState {
            projections: projections.clone(),
        })
    }

    fn create_pull_partition_states(
        op_state: &Self::OperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<Self::PartitionState>> {
        let generator = RegionGenerator::new();
        unimplemented!()
    }

    fn poll_pull(
        cx: &mut std::task::Context,
        op_state: &Self::OperatorState,
        state: &mut Self::PartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unimplemented!()
    }
}
