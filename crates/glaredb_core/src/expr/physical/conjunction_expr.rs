use std::fmt::{self, Debug};

use glaredb_error::Result;

use super::evaluator::ExpressionState;
use super::scalar_function_expr::PhysicalScalarFunctionExpr;
use crate::arrays::array::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::expr::conjunction_expr::ConjunctionOperator;

/// Wrapper around a scalar function representing a conjunction.
///
/// Lets us specialize selection execution for short-circuiting.
#[derive(Debug, Clone)]
pub struct PhysicalConjunctionExpr {
    /// The conjunction operator.
    pub op: ConjunctionOperator,
    /// The scalar function expression representing the conjunction.
    pub expr: PhysicalScalarFunctionExpr,
}

impl PhysicalConjunctionExpr {
    pub(crate) fn create_state(&self, batch_size: usize) -> Result<ExpressionState> {
        self.expr.create_state(batch_size)
    }

    pub fn datatype(&self) -> DataType {
        self.expr.datatype()
    }

    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        state: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        self.expr.eval(input, state, sel, output)
    }
}

impl fmt::Display for PhysicalConjunctionExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.expr)
    }
}
