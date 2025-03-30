use std::any::Any;
use std::sync::Arc;

use crate::arrays::datatype::DataType;
use crate::expr::Expression;

#[derive(Debug, Clone)]
pub struct RawBindState {
    pub state: Arc<dyn Any + Sync + Send>,
    pub return_type: DataType,
    pub inputs: Vec<Expression>,
}

impl RawBindState {
    pub(crate) fn state_as_any(&self) -> &dyn Any {
        self.state.as_ref()
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
