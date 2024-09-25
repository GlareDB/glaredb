use fmtutil::IntoDisplayableSlice;

use crate::functions::scalar::PlannedScalarFunction;
use std::fmt;

use super::Expression;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScalarFunctionExpr {
    pub function: Box<dyn PlannedScalarFunction>,
    pub inputs: Vec<Expression>,
}

impl fmt::Display for ScalarFunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.function.scalar_function().name(),
            self.inputs.display_as_list()
        )
    }
}
