pub mod builtin;

use std::any::Any;
use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use glaredb_error::Result;

use super::Signature;
use super::bind_state::{BindState, RawBindState};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::expr::Expression;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionVolatility {
    /// Every call to this function with the same arguemnts is not guaranteed to
    /// return the same value.
    Volatile,
    /// This function is consistent within the query.
    Consistent,
}

/// A scalar function that has an associated bind state.
///
/// # Safety
///
/// The bind state must be the result of a `bind` call from the raw scalar
/// function on this struct.
#[derive(Debug, Clone)]
pub struct PlannedScalarFunction {
    /// Name of this function.
    pub(crate) name: &'static str,
    /// The raw function containing the vtable to call into.
    pub(crate) raw: &'static RawScalarFunction,
    /// State for the function (inputs, return type).
    pub(crate) state: RawBindState,
}

impl PlannedScalarFunction {
    pub fn call_execute(&self, batch: &Batch, output: &mut Array) -> Result<()> {
        unsafe { (self.raw.vtable.execute_fn)(self.state.state_as_any(), batch, output) }
    }
}

/// Assumes that a function with same inputs and return type is using the same
/// function implementation.
impl PartialEq for PlannedScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.state.return_type == other.state.return_type
            && self.state.inputs == other.state.inputs
    }
}

impl Eq for PlannedScalarFunction {}

impl Hash for PlannedScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.state.return_type.hash(state);
        self.state.inputs.hash(state);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RawScalarFunctionVTable {
    /// Create the function state and compute the return type.
    bind_fn: unsafe fn(function: *const (), inputs: Vec<Expression>) -> Result<RawBindState>,
    /// Execute the function. First argument is the state.
    execute_fn: unsafe fn(state: &dyn Any, input: &Batch, output: &mut Array) -> Result<()>,
}

#[derive(Debug, Clone, Copy)]
pub struct RawScalarFunction {
    function: *const (),
    signature: &'static Signature,
    volatility: FunctionVolatility,
    vtable: &'static RawScalarFunctionVTable,
}

unsafe impl Send for RawScalarFunction {}
unsafe impl Sync for RawScalarFunction {}

impl RawScalarFunction {
    pub const fn new<F>(sig: &'static Signature, function: &'static F) -> Self
    where
        F: ScalarFunction,
    {
        let function = (function as *const F).cast();
        RawScalarFunction {
            function,
            signature: sig,
            volatility: F::VOLATILITY,
            vtable: F::VTABLE,
        }
    }

    pub fn call_bind(&self, inputs: Vec<Expression>) -> Result<RawBindState> {
        unsafe { (self.vtable.bind_fn)(self.function, inputs) }
    }

    pub fn signature(&self) -> &'static Signature {
        self.signature
    }

    pub fn volatility(&self) -> FunctionVolatility {
        self.volatility
    }
}

pub trait ScalarFunction: Copy + Debug + Sync + Send + Sized + 'static {
    const VOLATILITY: FunctionVolatility = FunctionVolatility::Consistent;

    /// State that gets passed to the function during execute.
    type State: Sync + Send;

    /// Compute the return type from the expression inputs and return a function
    /// state.
    ///
    /// This will only be called with expressions that match the signature this
    /// function was registered with.
    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>>;

    /// Execute the function the input batch, writing the output for each row
    /// into `output` at the same index.
    ///
    /// `output` has the following guarantees:
    /// - Has at least the primary buffer capacity needed to write the results.
    /// - All validities are initalized to 'valid'.
    /// - Array data can be written to.
    ///
    /// The batch's `selection` method should be called to determine which rows
    /// should be looked at during function eval.
    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()>;
}

trait ScalarFunctionVTable: ScalarFunction {
    const VTABLE: &'static RawScalarFunctionVTable = &RawScalarFunctionVTable {
        bind_fn: |function: *const (), inputs: Vec<Expression>| -> Result<RawBindState> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = function.bind(inputs)?;

            Ok(RawBindState {
                state: Arc::new(state.state),
                return_type: state.return_type,
                inputs: state.inputs,
            })
        },
        execute_fn: |state: &dyn Any, input: &Batch, output: &mut Array| -> Result<()> {
            let state = state.downcast_ref::<Self::State>().unwrap();
            Self::execute(state, input, output)
        },
    };
}

impl<F> ScalarFunctionVTable for F where F: ScalarFunction {}
