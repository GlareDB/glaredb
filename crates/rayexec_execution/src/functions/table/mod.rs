pub mod builtin;
pub mod execute;
pub mod multi_file;
pub mod scan;

use std::collections::HashMap;
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::Context;

use execute::TableExecuteFunction;
use rayexec_error::{RayexecError, Result};
use scan::TableScanFunction;

use super::Signature;
use crate::arrays::batch::Batch;
use crate::arrays::field::Schema;
use crate::catalog::context::DatabaseContext;
use crate::execution::operators::{ExecutionProperties, PollExecute, PollFinalize, PollPull};
use crate::expr::Expression;
use crate::logical::statistics::StatisticsValue;
use crate::ptr::raw_clone_ptr::RawClonePtr;
use crate::ptr::raw_ptr::RawPtr;
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
    pub state: RawClonePtr,
    pub input: TableFunctionInput,
    pub schema: Schema,
    pub cardinality: StatisticsValue<usize>,
}

#[derive(Debug)]
pub struct TableFunctionBindState<S> {
    pub state: S,
    pub input: TableFunctionInput,
    pub schema: Schema,
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
pub struct RawTableOperatorState(RawClonePtr);

#[derive(Debug)]
pub struct RawTablePartitionState(RawPtr);

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
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState> {
        // SAFETY: The pointer we pass to the bind fn is the pointer we get from
        // the static reference we use to construct this object.
        let fut = unsafe { (self.vtable.scan_bind_fn)(self.function, db_context, input)? };
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
        projections: &Projections,
        props: ExecutionProperties,
    ) -> Result<RawTableOperatorState> {
        unsafe {
            (self.vtable.create_pull_operator_state_fn)(bind_state.state.get(), projections, props)
        }
    }

    pub fn call_create_pull_partition_states(
        &self,
        op_state: &RawTableOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawTablePartitionState>> {
        unsafe {
            (self.vtable.create_pull_partition_states_fn)(op_state.0.get(), props, partitions)
        }
    }

    pub fn call_create_execute_operator_state(
        &self,
        bind_state: &RawTableFunctionBindState,
        props: ExecutionProperties,
    ) -> Result<RawTableOperatorState> {
        unsafe { (self.vtable.create_execute_operator_state_fn)(bind_state.state.get(), props) }
    }

    pub fn call_create_execute_partition_states(
        &self,
        op_state: &RawTableOperatorState,
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawTablePartitionState>> {
        unsafe {
            (self.vtable.create_execute_partition_states_fn)(op_state.0.get(), props, partitions)
        }
    }

    pub fn call_poll_execute(
        &self,
        cx: &mut Context,
        op_state: &RawTableOperatorState,
        partition_state: &mut RawTablePartitionState,
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute> {
        unsafe {
            (self.vtable.poll_execute_fn)(
                cx,
                op_state.0.get(),
                partition_state.0.get_mut(),
                input,
                output,
            )
        }
    }

    pub fn call_poll_pull(
        &self,
        cx: &mut Context,
        op_state: &RawTableOperatorState,
        partition_state: &mut RawTablePartitionState,
        output: &mut Batch,
    ) -> Result<PollPull> {
        unsafe {
            (self.vtable.poll_pull_fn)(cx, op_state.0.get(), partition_state.0.get_mut(), output)
        }
    }

    pub fn call_poll_finalize_execute(
        &self,
        cx: &mut Context,
        op_state: &RawTableOperatorState,
        partition_state: &mut RawTablePartitionState,
    ) -> Result<PollFinalize> {
        unsafe {
            (self.vtable.poll_finalize_execute_fn)(
                cx,
                op_state.0.get(),
                partition_state.0.get_mut(),
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

type ScanBindFut<'a> =
    Pin<Box<dyn Future<Output = Result<RawTableFunctionBindState>> + Sync + Send + 'a>>;

#[derive(Debug, Clone, Copy)]
pub struct RawTableFunctionVTable {
    scan_bind_fn: unsafe fn(
        function: *const (),
        db_context: &DatabaseContext,
        input: TableFunctionInput,
    ) -> Result<ScanBindFut>,

    execute_bind_fn: unsafe fn(
        function: *const (),
        input: TableFunctionInput,
    ) -> Result<RawTableFunctionBindState>,

    create_pull_operator_state_fn: unsafe fn(
        bind_state: *const (),
        projections: &Projections,
        props: ExecutionProperties,
    ) -> Result<RawTableOperatorState>,

    create_pull_partition_states_fn: unsafe fn(
        op_state: *const (),
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawTablePartitionState>>,

    create_execute_operator_state_fn: unsafe fn(
        bind_state: *const (),
        props: ExecutionProperties,
    ) -> Result<RawTableOperatorState>,

    create_execute_partition_states_fn: unsafe fn(
        op_state: *const (),
        props: ExecutionProperties,
        partitions: usize,
    ) -> Result<Vec<RawTablePartitionState>>,

    poll_execute_fn: unsafe fn(
        cx: &mut Context,
        op_state: *const (),
        partition_state: *mut (),
        input: &mut Batch,
        output: &mut Batch,
    ) -> Result<PollExecute>,

    poll_finalize_execute_fn: unsafe fn(
        cx: &mut Context,
        op_state: *const (),
        partition_state: *mut (),
    ) -> Result<PollFinalize>,

    poll_pull_fn: unsafe fn(
        cx: &mut Context,
        op_state: *const (),
        partition_state: *mut (),
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
        scan_bind_fn: |function, db_context, input| Err(RayexecError::new("Not a scan function")),
        execute_bind_fn: |function, input| {
            let function = unsafe { function.cast::<F>().as_ref().unwrap() };
            let state = function.bind(input)?;
            let raw = RawClonePtr::new(state.state);

            Ok(RawTableFunctionBindState {
                state: raw,
                input: state.input,
                schema: state.schema,
                cardinality: state.cardinality,
            })
        },
        create_pull_operator_state_fn: |bind_state, projections, props| {
            Err(RayexecError::new("Not a scan function"))
        },
        create_pull_partition_states_fn: |bind_state, props, partitions| {
            Err(RayexecError::new("Not a scan function"))
        },
        create_execute_operator_state_fn: |bind_state, props| {
            let bind_state = unsafe {
                bind_state
                    .cast::<<F as TableExecuteFunction>::BindState>()
                    .as_ref()
                    .unwrap()
            };
            let op_state = F::create_execute_operator_state(bind_state, props)?;
            Ok(RawTableOperatorState(RawClonePtr::new(op_state)))
        },
        create_execute_partition_states_fn: |op_state, props, partitions| {
            let op_state = unsafe {
                op_state
                    .cast::<<F as TableExecuteFunction>::OperatorState>()
                    .as_ref()
                    .unwrap()
            };
            let states = F::create_execute_partition_states(op_state, props, partitions)?;
            let states = states
                .into_iter()
                .map(|state| RawTablePartitionState(RawPtr::new(state)))
                .collect();

            Ok(states)
        },

        poll_execute_fn: |cx, op_state, partition_state, input, output| {
            let op_state = unsafe {
                op_state
                    .cast::<<F as TableExecuteFunction>::OperatorState>()
                    .as_ref()
                    .unwrap()
            };
            let partition_state = unsafe {
                partition_state
                    .cast::<<F as TableExecuteFunction>::PartitionState>()
                    .as_mut()
                    .unwrap()
            };
            F::poll_execute(cx, op_state, partition_state, input, output)
        },
        poll_finalize_execute_fn: |cx, op_state, partition_state| {
            let op_state = unsafe {
                op_state
                    .cast::<<F as TableExecuteFunction>::OperatorState>()
                    .as_ref()
                    .unwrap()
            };
            let partition_state = unsafe {
                partition_state
                    .cast::<<F as TableExecuteFunction>::PartitionState>()
                    .as_mut()
                    .unwrap()
            };
            F::poll_finalize_execute(cx, op_state, partition_state)
        },

        poll_pull_fn: |cx, op_state, partition_state, output| {
            Err(RayexecError::new("Not a scan functions"))
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
        scan_bind_fn: |function, db_context, input| {
            let function = unsafe { function.cast::<F>().as_ref().unwrap() };
            Ok(Box::pin(async {
                let state = function.bind(db_context, input).await?;
                let raw = RawClonePtr::new(state.state);

                Ok(RawTableFunctionBindState {
                    state: raw,
                    input: state.input,
                    schema: state.schema,
                    cardinality: state.cardinality,
                })
            }))
        },
        execute_bind_fn: |function, input| Err(RayexecError::new("Not an execute function")),
        create_pull_operator_state_fn: |bind_state, projections, props| {
            let bind_state = unsafe {
                bind_state
                    .cast::<<F as TableScanFunction>::BindState>()
                    .as_ref()
                    .unwrap()
            };
            let op_state = F::create_pull_operator_state(bind_state, projections, props)?;
            Ok(RawTableOperatorState(RawClonePtr::new(op_state)))
        },
        create_pull_partition_states_fn: |op_state, props, partitions| {
            let op_state = unsafe {
                op_state
                    .cast::<<F as TableScanFunction>::OperatorState>()
                    .as_ref()
                    .unwrap()
            };
            let states = F::create_pull_partition_states(op_state, props, partitions)?;
            let states = states
                .into_iter()
                .map(|state| RawTablePartitionState(RawPtr::new(state)))
                .collect();

            Ok(states)
        },
        create_execute_operator_state_fn: |bind_state, props| {
            Err(RayexecError::new("Not an execute function"))
        },
        create_execute_partition_states_fn: |op_state, props, partitions| {
            Err(RayexecError::new("Not an execute function"))
        },

        poll_execute_fn: |cx, op_state, partition_state, input, output| {
            Err(RayexecError::new("Not an execute function"))
        },
        poll_finalize_execute_fn: |cx, op_state, partition_state| {
            Err(RayexecError::new("Not an execute function"))
        },

        poll_pull_fn: |cx, op_state, partition_state, output| {
            let op_state = unsafe {
                op_state
                    .cast::<<F as TableScanFunction>::OperatorState>()
                    .as_ref()
                    .unwrap()
            };
            let partition_state = unsafe {
                partition_state
                    .cast::<<F as TableScanFunction>::PartitionState>()
                    .as_mut()
                    .unwrap()
            };
            F::poll_pull(cx, op_state, partition_state, output)
        },
    };
}
