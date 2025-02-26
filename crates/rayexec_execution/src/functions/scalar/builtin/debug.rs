use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::expr::Expression;
use crate::functions::scalar::{BindState, ScalarFunction};

/// A function implementation that errors during bind and execute indicating
/// something is not implemented.
#[derive(Debug, Clone, Copy)]
pub struct ScalarNotImplemented {
    name: &'static str,
}

impl ScalarNotImplemented {
    pub const fn new(name: &'static str) -> Self {
        ScalarNotImplemented { name }
    }
}

impl ScalarFunction for ScalarNotImplemented {
    type State = ();

    fn bind(&self, _inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        Err(RayexecError::new(format!(
            "Scalar function '{}' not yet implemnted",
            self.name
        )))
    }

    fn execute(_state: &Self::State, _input: &Batch, _output: &mut Array) -> Result<()> {
        Err(RayexecError::new("Scalar function not implemented"))
    }
}
