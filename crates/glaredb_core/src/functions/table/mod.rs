pub mod builtin;
pub mod execute;
pub mod scan;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use execute::TableExecuteFunction;
use glaredb_error::{DbError, Result};
use scan::{ScanContext, TableScanFunction};

use super::Signature;
use crate::arrays::batch::Batch;
use crate::arrays::field::ColumnSchema;
use crate::execution::operators::{ExecutionProperties, PollExecute, PollFinalize, PollPull};
use crate::expr::Expression;
use crate::statistics::value::StatisticsValue;
use crate::storage::projections::Projections;
use crate::storage::scan_filter::PhysicalScanFilter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFunctionInput {
    pub positional: Vec<Expression>,
    pub named: HashMap<String, Expression>,
}

impl TableFunctionInput {
    pub fn all_unnamed<E>(exprs: impl IntoIterator<Item = E>) -> Self
    where
        E: Into<Expression>,
    {
        TableFunctionInput {
            positional: exprs.into_iter().map(|e| e.into()).collect(),
            named: HashMap::new(),
        }
    }
}

// TODO: Chunky (152)
//
// Should we just arc all of it?
#[derive(Debug, Clone)]
pub struct RawTableFunctionBindState {
    pub(crate) state: Arc<dyn Any + Sync + Send>,
    pub(crate) input: TableFunctionInput,
    pub(crate) data_schema: ColumnSchema,
    pub(crate) meta_schema: Option<ColumnSchema>,
    pub(crate) cardinality: StatisticsValue<usize>,
}

#[derive(Debug)]
pub struct TableFunctionBindState<S> {
    /// Any state needed for the function.
    pub state: S,
    /// Inputs the to function.
    pub input: TableFunctionInput,
    /// Output schema of the function. This should be the schema of the "file"
    /// or returned table.
    pub data_schema: ColumnSchema,
    /// Output schema of the metadata, if available.
    ///
    /// This should be the column schema for metadata columns, e.g. columns
    /// provided by a multi file scan.
    pub meta_schema: Option<ColumnSchema>,
    /// Output cardinality.
    pub cardinality: StatisticsValue<usize>,
}

#[derive(Debug, Clone)]
pub struct PlannedTableFunction {
    pub(crate) name: &'static str,
    pub(crate) raw: &'static RawTableFunction,
    pub(crate) bind_state: RawTableFunctionBindState,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedTableFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.bind_state.data_schema == other.bind_state.data_schema
            && self.bind_state.meta_schema == other.bind_state.meta_schema
            && self.bind_state.input == other.bind_state.input
    }
}

impl Eq for PlannedTableFunction {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableFunctionType {
    Scan,
    Execute,
}

#[derive(Debug, Clone)]
pub struct AnyTableOperatorState(Arc<dyn Any + Sync + Send>);

#[derive(Debug)]
pub struct AnyTablePartitionState(Box<dyn Any + Sync + Send>);

/// A raw table function contains the vtable for the function implementation
/// alongside a signature.
///
/// # Safety
///
/// All public methods are safe.
///
/// All crate visible methods that accept various states (bind state, op state,
/// partition state) are unsafe as they require the states passed to the methods
/// to be the correct underlying type.
#[derive(Debug, Clone, Copy)]
pub struct RawTableFunction {
    signature: &'static Signature,
    vtable: &'static RawTableFunctionVTable,
    function_type: TableFunctionType,
}

unsafe impl Send for RawTableFunction {}
unsafe impl Sync for RawTableFunction {}

// TODO: Remove `function` args.
impl RawTableFunction {
    pub const fn new_execute<F>(sig: &'static Signature, _function: &'static F) -> Self
    where
        F: TableExecuteFunction,
    {
        RawTableFunction {
            signature: sig,
            vtable: TableExecuteVTable::<F>::VTABLE,
            function_type: TableExecuteVTable::<F>::FUNCTION_TYPE,
        }
    }

    pub const fn new_scan<F>(sig: &'static Signature, _function: &'static F) -> Self
    where
        F: TableScanFunction,
    {
        RawTableFunction {
            signature: sig,
            vtable: TableScanVTable::<F>::VTABLE,
            function_type: TableScanVTable::<F>::FUNCTION_TYPE,
        }
    }

    pub const fn function_type(&self) -> TableFunctionType {
        self.function_type
    }

    pub const fn signature(&self) -> &Signature {
        self.signature
    }

    pub(crate) async fn call_scan_bind(
        &self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState> {
        // SAFETY: The pointer we pass to the bind fn is the pointer we get from
        // the static reference we use to construct this object.
        let fut = unsafe { (self.vtable.scan_bind_fn)(scan_context, input)? };
        fut.await
    }

    pub(crate) fn call_execute_bind(
        &self,
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState> {
        unsafe { (self.vtable.execute_bind_fn)(input) }
    }

    pub(crate) unsafe fn call_create_pull_operator_state(
        &self,
        bind_state: &RawTableFunctionBindState,
        projections: Projections,
        filters: &[PhysicalScanFilter],
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState> {
        unsafe {
            (self.vtable.create_pull_operator_state_fn)(
                bind_state.state.as_ref(),
                projections,
                filters,
                props,
            )
        }
    }

    pub(crate) unsafe fn call_create_pull_partition_states(
        &self,
        bind_state: &RawTableFunctionBindState,
        op_state: &AnyTableOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>> {
        unsafe {
            (self.vtable.create_pull_partition_states_fn)(
                bind_state.state.as_ref(),
                op_state.0.as_ref(),
                props,
                partitions,
            )
        }
    }

    pub(crate) unsafe fn call_create_execute_operator_state(
        &self,
        bind_state: &RawTableFunctionBindState,
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState> {
        unsafe { (self.vtable.create_execute_operator_state_fn)(bind_state.state.as_ref(), props) }
    }

    pub(crate) unsafe fn call_create_execute_partition_states(
        &self,
        bind_state: &RawTableFunctionBindState,
        op_state: &AnyTableOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>> {
        unsafe {
            (self.vtable.create_execute_partition_states_fn)(
                bind_state.state.as_ref(),
                op_state.0.as_ref(),
                props,
                partitions,
            )
        }
    }

    pub(crate) unsafe fn call_poll_execute(
        &self,
        cx: &mut Context,
        bind_state: &RawTableFunctionBindState,
        op_state: &AnyTableOperatorState,
        partition_state: &mut AnyTablePartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        unsafe {
            (self.vtable.poll_execute_fn)(
                cx,
                bind_state.state.as_ref(),
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
                input,
                output,
            )
        }
    }

    pub(crate) unsafe fn call_poll_pull(
        &self,
        cx: &mut Context,
        bind_state: &RawTableFunctionBindState,
        op_state: &AnyTableOperatorState,
        partition_state: &mut AnyTablePartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unsafe {
            (self.vtable.poll_pull_fn)(
                cx,
                bind_state.state.as_ref(),
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
                output,
            )
        }
    }

    pub(crate) unsafe fn call_poll_finalize_execute(
        &self,
        cx: &mut Context,
        bind_state: &RawTableFunctionBindState,
        op_state: &AnyTableOperatorState,
        partition_state: &mut AnyTablePartitionState,
    ) -> Result<PollFinalize> {
        unsafe {
            (self.vtable.poll_finalize_execute_fn)(
                cx,
                bind_state.state.as_ref(),
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
            )
        }
    }
}

type ScanBindFut<'a> = Pin<Box<dyn Future<Output = Result<RawTableFunctionBindState>> + Send + 'a>>;

#[derive(Debug, Clone, Copy)]
#[allow(clippy::type_complexity)]
struct RawTableFunctionVTable {
    scan_bind_fn:
        unsafe fn(scan_context: ScanContext, input: TableFunctionInput) -> Result<ScanBindFut>,

    execute_bind_fn: unsafe fn(input: TableFunctionInput) -> Result<RawTableFunctionBindState>,

    create_pull_operator_state_fn: unsafe fn(
        bind_state: &dyn Any,
        projections: Projections,
        filters: &[PhysicalScanFilter],
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState>,

    create_pull_partition_states_fn: unsafe fn(
        bind_state: &dyn Any,
        op_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>>,

    create_execute_operator_state_fn: unsafe fn(
        bind_state: &dyn Any,
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState>,

    create_execute_partition_states_fn: unsafe fn(
        bind_state: &dyn Any,
        op_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>>,

    poll_execute_fn: unsafe fn(
        cx: &mut Context,
        bind_state: &dyn Any,
        op_state: &dyn Any,
        partition_state: &mut dyn Any,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>,

    poll_finalize_execute_fn: unsafe fn(
        cx: &mut Context,
        bind_state: &dyn Any,
        op_state: &dyn Any,
        partition_state: &mut dyn Any,
    ) -> Result<PollFinalize>,

    poll_pull_fn: unsafe fn(
        cx: &mut Context,
        bind_state: &dyn Any,
        op_state: &dyn Any,
        partition_state: &mut dyn Any,
        output: &mut Batch,
    ) -> Result<PollPull>,
}

trait TableFunctionVTable {
    const FUNCTION_TYPE: TableFunctionType;
    const VTABLE: &'static RawTableFunctionVTable;
}

struct TableExecuteVTable<F: TableExecuteFunction>(PhantomData<F>);

impl<F> TableFunctionVTable for TableExecuteVTable<F>
where
    F: TableExecuteFunction,
{
    const FUNCTION_TYPE: TableFunctionType = TableFunctionType::Execute;

    const VTABLE: &'static RawTableFunctionVTable = &RawTableFunctionVTable {
        scan_bind_fn: |_db_context, _input| Err(DbError::new("Not a scan function")),
        execute_bind_fn: |input| {
            let state = F::bind(input)?;

            Ok(RawTableFunctionBindState {
                state: Arc::new(state.state),
                input: state.input,
                data_schema: state.data_schema,
                meta_schema: state.meta_schema,
                cardinality: state.cardinality,
            })
        },
        create_pull_operator_state_fn: |_bind_state, _projections, _filters, _props| {
            Err(DbError::new("Not a scan function"))
        },
        create_pull_partition_states_fn: |_bind_state, _op_state, _props, _partitions| {
            Err(DbError::new("Not a scan function"))
        },
        create_execute_operator_state_fn: |bind_state, props| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableExecuteFunction>::BindState>()
                .unwrap();
            let op_state = F::create_execute_operator_state(bind_state, props)?;
            Ok(AnyTableOperatorState(Arc::new(op_state)))
        },
        create_execute_partition_states_fn: |bind_state, op_state, props, partitions| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableExecuteFunction>::BindState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<F as TableExecuteFunction>::OperatorState>()
                .unwrap();
            let states =
                F::create_execute_partition_states(bind_state, op_state, props, partitions)?;
            let states = states
                .into_iter()
                .map(|state| AnyTablePartitionState(Box::new(state)))
                .collect();

            Ok(states)
        },

        poll_execute_fn: |cx, bind_state, op_state, partition_state, input, output| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableExecuteFunction>::BindState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<F as TableExecuteFunction>::OperatorState>()
                .unwrap();
            let partition_state = partition_state
                .downcast_mut::<<F as TableExecuteFunction>::PartitionState>()
                .unwrap();
            F::poll_execute(cx, bind_state, op_state, partition_state, input, output)
        },
        poll_finalize_execute_fn: |cx, bind_state, op_state, partition_state| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableExecuteFunction>::BindState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<F as TableExecuteFunction>::OperatorState>()
                .unwrap();
            let partition_state = partition_state
                .downcast_mut::<<F as TableExecuteFunction>::PartitionState>()
                .unwrap();
            F::poll_finalize_execute(cx, bind_state, op_state, partition_state)
        },

        poll_pull_fn: |_cx, _bind_state, _op_state, _partition_state, _output| {
            Err(DbError::new("Not a scan functions"))
        },
    };
}

struct TableScanVTable<F: TableScanFunction>(PhantomData<F>);

impl<F> TableFunctionVTable for TableScanVTable<F>
where
    F: TableScanFunction,
{
    const FUNCTION_TYPE: TableFunctionType = TableFunctionType::Scan;

    const VTABLE: &'static RawTableFunctionVTable = &RawTableFunctionVTable {
        scan_bind_fn: |scan_context, input| {
            Ok(Box::pin(async move {
                let state = F::bind(scan_context, input).await?;

                Ok(RawTableFunctionBindState {
                    state: Arc::new(state.state),
                    input: state.input,
                    data_schema: state.data_schema,
                    meta_schema: state.meta_schema,
                    cardinality: state.cardinality,
                })
            }))
        },
        execute_bind_fn: |_input| Err(DbError::new("Not an execute function")),
        create_pull_operator_state_fn: |bind_state, projections, filters, props| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableScanFunction>::BindState>()
                .unwrap();
            let op_state = F::create_pull_operator_state(bind_state, projections, filters, props)?;
            Ok(AnyTableOperatorState(Arc::new(op_state)))
        },
        create_pull_partition_states_fn: |bind_state, op_state, props, partitions| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableScanFunction>::BindState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<F as TableScanFunction>::OperatorState>()
                .unwrap();
            let states = F::create_pull_partition_states(bind_state, op_state, props, partitions)?;
            let states = states
                .into_iter()
                .map(|state| AnyTablePartitionState(Box::new(state)))
                .collect();

            Ok(states)
        },
        create_execute_operator_state_fn: |_bind_state, _props| {
            Err(DbError::new("Not an execute function"))
        },
        create_execute_partition_states_fn: |_bind_state, _op_state, _props, _partitions| {
            Err(DbError::new("Not an execute function"))
        },

        poll_execute_fn: |_cx, _bind_state, _op_state, _partition_state, _input, _output| {
            Err(DbError::new("Not an execute function"))
        },
        poll_finalize_execute_fn: |_cx, _bind_state, _op_state, _partition_state| {
            Err(DbError::new("Not an execute function"))
        },

        poll_pull_fn: |cx, bind_state, op_state, partition_state, output| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableScanFunction>::BindState>()
                .unwrap();
            let op_state = op_state
                .downcast_ref::<<F as TableScanFunction>::OperatorState>()
                .unwrap();
            let partition_state = partition_state
                .downcast_mut::<<F as TableScanFunction>::PartitionState>()
                .unwrap();
            F::poll_pull(cx, bind_state, op_state, partition_state, output)
        },
    };
}
