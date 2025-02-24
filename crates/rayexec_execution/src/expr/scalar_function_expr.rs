use std::fmt;

use fmtutil::IntoDisplayableSlice;

use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::scalar::PlannedScalarFunction;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScalarFunctionExpr {
    pub function: PlannedScalarFunction,
}

impl From<PlannedScalarFunction> for ScalarFunctionExpr {
    fn from(value: PlannedScalarFunction) -> Self {
        ScalarFunctionExpr { function: value }
    }
}

impl ContextDisplay for ScalarFunctionExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let inputs: Vec<_> = self
            .function
            .state
            .inputs
            .iter()
            .map(|expr| ContextDisplayWrapper::with_mode(expr, mode))
            .collect();
        write!(f, "{}({})", self.function.name, inputs.display_as_list())
    }
}
