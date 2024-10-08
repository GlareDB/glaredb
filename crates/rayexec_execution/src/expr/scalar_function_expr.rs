use fmtutil::IntoDisplayableSlice;

use crate::{
    explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper},
    functions::scalar::PlannedScalarFunction,
};
use std::fmt;

use super::Expression;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScalarFunctionExpr {
    pub function: Box<dyn PlannedScalarFunction>,
    pub inputs: Vec<Expression>,
}

impl ContextDisplay for ScalarFunctionExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        let inputs: Vec<_> = self
            .inputs
            .iter()
            .map(|expr| ContextDisplayWrapper::with_mode(expr, mode))
            .collect();
        write!(
            f,
            "{}({})",
            self.function.scalar_function().name(),
            inputs.display_as_list()
        )
    }
}
