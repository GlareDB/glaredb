pub mod builtin;

use std::fmt::Debug;
use std::hash::Hash;
use std::sync::Arc;

use rayexec_error::Result;

use super::{FunctionInfo, Signature};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
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
    pub(crate) raw: RawScalarFunction,
    /// State for the function (inputs, return type).
    pub(crate) state: RawBindState,
}

impl PlannedScalarFunction {
    pub fn call_execute(&self, batch: &Batch, output: &mut Array) -> Result<()> {
        unsafe {
            (self.raw.vtable.execute_fn)(self.raw.function, self.state.state.0.ptr, batch, output)
        }
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
    /// Execute the function. First argument is a pointer to the function state.
    execute_fn: unsafe fn(
        function: *const (),
        state: *const (),
        input: &Batch,
        output: &mut Array,
    ) -> Result<()>,
}

#[derive(Debug, Clone, Copy)]
pub struct RawScalarFunction {
    function: *const (),
    signature: Signature,
    volatility: FunctionVolatility,
    vtable: &'static RawScalarFunctionVTable,
}

unsafe impl Send for RawScalarFunction {}
unsafe impl Sync for RawScalarFunction {}

impl RawScalarFunction {
    pub const fn new<F>(sig: Signature, function: &'static F) -> Self
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

    pub fn signature(&self) -> &Signature {
        &self.signature
    }

    pub fn volatility(&self) -> FunctionVolatility {
        self.volatility
    }
}

#[derive(Debug, Clone)]
pub struct RawBindState {
    pub state: RawScalarFunctionState,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

/// Bind state for a function. Paramterized on the function state.
#[derive(Debug)]
pub struct BindState<S> {
    pub state: S,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

/// State passed to functions during execute.
///
/// Inner state is wrapped in an arc since we allow cloning functions as we
/// allow cloning expressions.
///
/// The state passed during execute is not mutable, so if we end up with
/// multiple function calls using the same state, that's fine.
#[derive(Debug, Clone)]
pub struct RawScalarFunctionState(Arc<StateInner>);

#[derive(Debug)]
struct StateInner {
    ptr: *const (),
    drop_fn: unsafe fn(ptr: *const ()),
}

// SAFETY: `ScalarFunction::State` has Sync+Send bounds.
unsafe impl Send for StateInner {}
unsafe impl Sync for StateInner {}

impl Drop for RawScalarFunctionState {
    fn drop(&mut self) {
        unsafe { (self.0.drop_fn)(self.0.ptr) }
    }
}

pub trait ScalarFunction: Debug + Sync + Send + Sized {
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
    fn execute(&self, state: &Self::State, input: &Batch, output: &mut Array) -> Result<()>;
}

trait ScalarFunctionVTable: ScalarFunction {
    const VTABLE: &'static RawScalarFunctionVTable = &RawScalarFunctionVTable {
        bind_fn: |function: *const (), inputs: Vec<Expression>| -> Result<RawBindState> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = function.bind(inputs)?;
            let ptr = (&state.state as *const Self::State).cast();
            std::mem::forget(state.state); // We'll handle dropping manually.

            let raw = RawScalarFunctionState(Arc::new(StateInner {
                ptr,
                drop_fn: |ptr: *const ()| {
                    let ptr = ptr.cast::<Self::State>().cast_mut();
                    unsafe { ptr.drop_in_place() };
                },
            }));

            Ok(RawBindState {
                state: raw,
                return_type: state.return_type,
                inputs: state.inputs,
            })
        },
        execute_fn: |function: *const (),
                     state: *const (),
                     input: &Batch,
                     output: &mut Array|
         -> Result<()> {
            let function = unsafe { function.cast::<Self>().as_ref().unwrap() };
            let state = unsafe { state.cast::<Self::State>().as_ref().unwrap() };
            function.execute(state, input, output)
        },
    };
}

impl<F> ScalarFunctionVTable for F where F: ScalarFunction {}
