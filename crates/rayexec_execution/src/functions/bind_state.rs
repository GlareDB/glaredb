use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::ptr::raw_clone_ptr::RawClonePtr;

#[derive(Debug, Clone)]
pub struct RawTableFunctionBindState {
    pub state: RawClonePtr,
}

/// Bind state for a table function.
#[derive(Debug)]
pub struct TableFunctionBindState<S> {
    pub state: S,
}

#[derive(Debug, Clone)]
pub struct RawBindState {
    pub state: RawClonePtr,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

impl RawBindState {
    pub(crate) fn state_ptr(&self) -> *const () {
        self.state.get()
    }
}

/// Bind state for a a scalar or aggregate function. Paramterized on the
/// function state.
#[derive(Debug)]
pub struct BindState<S> {
    pub state: S,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}
