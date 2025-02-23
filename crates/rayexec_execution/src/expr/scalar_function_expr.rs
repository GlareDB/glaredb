use std::fmt;

use fmtutil::IntoDisplayableSlice;

use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::scalar::PlannedScalarFunction2;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScalarFunctionExpr {
    pub function: PlannedScalarFunction2,
}

impl From<PlannedScalarFunction2> for ScalarFunctionExpr {
    fn from(value: PlannedScalarFunction2) -> Self {
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
            .inputs
            .iter()
            .map(|expr| ContextDisplayWrapper::with_mode(expr, mode))
            .collect();
        write!(
            f,
            "{}({})",
            self.function.function.name(),
            inputs.display_as_list()
        )
    }
}
