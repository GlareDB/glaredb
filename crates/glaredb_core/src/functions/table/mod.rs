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
use crate::logical::statistics::StatisticsValue;
use crate::storage::projections::Projections;

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

#[derive(Debug, Clone)]
pub struct RawTableFunctionBindState {
    pub state: Arc<dyn Any + Sync + Send>,
    pub input: TableFunctionInput,
    pub schema: ColumnSchema,
    pub cardinality: StatisticsValue<usize>,
}

#[derive(Debug)]
pub struct TableFunctionBindState<S> {
    pub state: S,
    pub input: TableFunctionInput,
    pub schema: ColumnSchema,
    pub cardinality: StatisticsValue<usize>,
}

#[derive(Debug, Clone)]
pub struct PlannedTableFunction {
    pub(crate) name: &'static str,
    pub(crate) raw: RawTableFunction,
    pub(crate) bind_state: RawTableFunctionBindState,
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedTableFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.bind_state.schema == other.bind_state.schema
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

#[derive(Debug, Clone, Copy)]
pub struct RawTableFunction {
    function: *const (),
    signature: &'static Signature,
    vtable: &'static RawTableFunctionVTable,
    function_type: TableFunctionType,
}

unsafe impl Send for RawTableFunction {}
unsafe impl Sync for RawTableFunction {}

impl RawTableFunction {
    pub const fn new_execute<F>(sig: &'static Signature, function: &'static F) -> Self
    where
        F: TableExecuteFunction,
    {
        let function = (function as *const F).cast();
        RawTableFunction {
            function,
            signature: sig,
            vtable: TableExecuteVTable::<F>::VTABLE,
            function_type: TableExecuteVTable::<F>::FUNCTION_TYPE,
        }
    }

    pub const fn new_scan<F>(sig: &'static Signature, function: &'static F) -> Self
    where
        F: TableScanFunction,
    {
        let function = (function as *const F).cast();
        RawTableFunction {
            function,
            signature: sig,
            vtable: TableScanVTable::<F>::VTABLE,
            function_type: TableScanVTable::<F>::FUNCTION_TYPE,
        }
    }

    pub async fn call_scan_bind(
        &self,
        scan_context: ScanContext<'_>,
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState> {
        // SAFETY: The pointer we pass to the bind fn is the pointer we get from
        // the static reference we use to construct this object.
        let fut = unsafe { (self.vtable.scan_bind_fn)(self.function, scan_context, input)? };
        fut.await
    }

    pub fn call_execute_bind(
        &self,
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState> {
        unsafe { (self.vtable.execute_bind_fn)(self.function, input) }
    }

    pub fn call_create_pull_operator_state(
        &self,
        bind_state: &RawTableFunctionBindState,
        projections: Projections,
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState> {
        unsafe {
            (self.vtable.create_pull_operator_state_fn)(
                bind_state.state.as_ref(),
                projections,
                props,
            )
        }
    }

    pub fn call_create_pull_partition_states(
        &self,
        op_state: &AnyTableOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>> {
        unsafe {
            (self.vtable.create_pull_partition_states_fn)(op_state.0.as_ref(), props, partitions)
        }
    }

    pub fn call_create_execute_operator_state(
        &self,
        bind_state: &RawTableFunctionBindState,
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState> {
        unsafe { (self.vtable.create_execute_operator_state_fn)(bind_state.state.as_ref(), props) }
    }

    pub fn call_create_execute_partition_states(
        &self,
        op_state: &AnyTableOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>> {
        unsafe {
            (self.vtable.create_execute_partition_states_fn)(op_state.0.as_ref(), props, partitions)
        }
    }

    pub fn call_poll_execute(
        &self,
        cx: &mut Context,
        op_state: &AnyTableOperatorState,
        partition_state: &mut AnyTablePartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        unsafe {
            (self.vtable.poll_execute_fn)(
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
                input,
                output,
            )
        }
    }

    pub fn call_poll_pull(
        &self,
        cx: &mut Context,
        op_state: &AnyTableOperatorState,
        partition_state: &mut AnyTablePartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unsafe {
            (self.vtable.poll_pull_fn)(cx, op_state.0.as_ref(), partition_state.0.as_mut(), output)
        }
    }

    pub fn call_poll_finalize_execute(
        &self,
        cx: &mut Context,
        op_state: &AnyTableOperatorState,
        partition_state: &mut AnyTablePartitionState,
    ) -> Result<PollFinalize> {
        unsafe {
            (self.vtable.poll_finalize_execute_fn)(
                cx,
                op_state.0.as_ref(),
                partition_state.0.as_mut(),
            )
        }
    }

    pub fn function_type(&self) -> TableFunctionType {
        self.function_type
    }

    pub fn signature(&self) -> &Signature {
        self.signature
    }
}

type ScanBindFut<'a> = Pin<Box<dyn Future<Output = Result<RawTableFunctionBindState>> + Send + 'a>>;

#[derive(Debug, Clone, Copy)]
pub struct RawTableFunctionVTable {
    scan_bind_fn: unsafe fn(
        function: *const (),
        scan_context: ScanContext,
        input: TableFunctionInput,
    ) -> Result<ScanBindFut>,

    execute_bind_fn: unsafe fn(
        function: *const (),
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState>,

    create_pull_operator_state_fn: unsafe fn(
        bind_state: &dyn Any,
        projections: Projections,
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState>,

    create_pull_partition_states_fn: unsafe fn(
        op_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>>,

    create_execute_operator_state_fn: unsafe fn(
        bind_state: &dyn Any,
        props: ExecutionProperties,
    ) -> Result<AnyTableOperatorState>,

    create_execute_partition_states_fn: unsafe fn(
        op_state: &dyn Any,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<AnyTablePartitionState>>,

    poll_execute_fn: unsafe fn(
        cx: &mut Context,
        op_state: &dyn Any,
        partition_state: &mut dyn Any,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>,

    poll_finalize_execute_fn: unsafe fn(
        cx: &mut Context,
        op_state: &dyn Any,
        partition_state: &mut dyn Any,
    ) -> Result<PollFinalize>,

    poll_pull_fn: unsafe fn(
        cx: &mut Context,
        op_state: &dyn Any,
        partition_state: &mut dyn Any,
        output: &mut Batch,
    ) -> Result<PollPull>,
}

// TODO: Seal
pub trait TableFunctionVTable {
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
        scan_bind_fn: |_function, _db_context, _input| Err(DbError::new("Not a scan function")),
        execute_bind_fn: |function, input| {
            let function = unsafe { function.cast::<F>().as_ref().unwrap() };
            let state = function.bind(input)?;

            Ok(RawTableFunctionBindState {
                state: Arc::new(state.state),
                input: state.input,
                schema: state.schema,
                cardinality: state.cardinality,
            })
        },
        create_pull_operator_state_fn: |_bind_state, _projections, _props| {
            Err(DbError::new("Not a scan function"))
        },
        create_pull_partition_states_fn: |_bind_state, _props, _partitions| {
            Err(DbError::new("Not a scan function"))
        },
        create_execute_operator_state_fn: |bind_state, props| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableExecuteFunction>::BindState>()
                .unwrap();
            let op_state = F::create_execute_operator_state(bind_state, props)?;
            Ok(AnyTableOperatorState(Arc::new(op_state)))
        },
        create_execute_partition_states_fn: |op_state, props, partitions| {
            let op_state = op_state
                .downcast_ref::<<F as TableExecuteFunction>::OperatorState>()
                .unwrap();
            let states = F::create_execute_partition_states(op_state, props, partitions)?;
            let states = states
                .into_iter()
                .map(|state| AnyTablePartitionState(Box::new(state)))
                .collect();

            Ok(states)
        },

        poll_execute_fn: |cx, op_state, partition_state, input, output| {
            let op_state = op_state
                .downcast_ref::<<F as TableExecuteFunction>::OperatorState>()
                .unwrap();
            let partition_state = partition_state
                .downcast_mut::<<F as TableExecuteFunction>::PartitionState>()
                .unwrap();
            F::poll_execute(cx, op_state, partition_state, input, output)
        },
        poll_finalize_execute_fn: |cx, op_state, partition_state| {
            let op_state = op_state
                .downcast_ref::<<F as TableExecuteFunction>::OperatorState>()
                .unwrap();
            let partition_state = partition_state
                .downcast_mut::<<F as TableExecuteFunction>::PartitionState>()
                .unwrap();
            F::poll_finalize_execute(cx, op_state, partition_state)
        },

        poll_pull_fn: |_cx, _op_state, _partition_state, _output| {
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
        scan_bind_fn: |function, scan_context, input| {
            let function = unsafe { function.cast::<F>().as_ref().unwrap() };
            Ok(Box::pin(async move {
                let state = function.bind(scan_context, input).await?;

                Ok(RawTableFunctionBindState {
                    state: Arc::new(state.state),
                    input: state.input,
                    schema: state.schema,
                    cardinality: state.cardinality,
                })
            }))
        },
        execute_bind_fn: |_function, _input| Err(DbError::new("Not an execute function")),
        create_pull_operator_state_fn: |bind_state, projections, props| {
            let bind_state = bind_state
                .downcast_ref::<<F as TableScanFunction>::BindState>()
                .unwrap();
            let op_state = F::create_pull_operator_state(bind_state, projections, props)?;
            Ok(AnyTableOperatorState(Arc::new(op_state)))
        },
        create_pull_partition_states_fn: |op_state, props, partitions| {
            let op_state = op_state
                .downcast_ref::<<F as TableScanFunction>::OperatorState>()
                .unwrap();
            let states = F::create_pull_partition_states(op_state, props, partitions)?;
            let states = states
                .into_iter()
                .map(|state| AnyTablePartitionState(Box::new(state)))
                .collect();

            Ok(states)
        },
        create_execute_operator_state_fn: |_bind_state, _props| {
            Err(DbError::new("Not an execute function"))
        },
        create_execute_partition_states_fn: |_op_state, _props, _partitions| {
            Err(DbError::new("Not an execute function"))
        },

        poll_execute_fn: |_cx, _op_state, _partition_state, _input, _output| {
            Err(DbError::new("Not an execute function"))
        },
        poll_finalize_execute_fn: |_cx, _op_state, _partition_state| {
            Err(DbError::new("Not an execute function"))
        },

        poll_pull_fn: |cx, op_state, partition_state, output| {
            let op_state = op_state
                .downcast_ref::<<F as TableScanFunction>::OperatorState>()
                .unwrap();
            let partition_state = partition_state
                .downcast_mut::<<F as TableScanFunction>::PartitionState>()
                .unwrap();
            F::poll_pull(cx, op_state, partition_state, output)
        },
    };
}
