pub mod scalar;

use crate::functions::scalar::SpecializedScalarFunction;
use crate::planner::operator::LogicalExpression;
use rayexec_bullet::field::{DataType, TypeSchema};
use rayexec_bullet::{array::Array, batch::Batch, scalar::OwnedScalarValue};
use rayexec_error::{RayexecError, Result};
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum PhysicalScalarExpression {
    /// Reference to a column in the input batch.
    Column(usize),

    /// A scalar literal.
    Literal(OwnedScalarValue),

    /// A scalar function.
    ScalarFunction {
        /// The specialized function we'll be calling.
        function: Box<dyn SpecializedScalarFunction>,

        /// Column inputs into the function.
        inputs: Vec<PhysicalScalarExpression>,
    },

    /// Case expressions.
    Case {
        input: Box<PhysicalScalarExpression>,
        /// When <left>, then <right>
        when_then: Vec<(PhysicalScalarExpression, PhysicalScalarExpression)>,
    },
}

impl PhysicalScalarExpression {
    /// Try to produce a physical expression from a logical expression.
    ///
    /// Errors if the expression is not scalar, or if it contains correlated
    /// columns (columns that reference an outer scope).
    pub fn try_from_uncorrelated_expr(
        logical: LogicalExpression,
        input: &TypeSchema,
    ) -> Result<Self> {
        Ok(match logical {
            LogicalExpression::ColumnRef(col) => {
                let col = col.try_as_uncorrelated()?;
                if col >= input.types.len() {
                    return Err(RayexecError::new(format!(
                        "Invalid column index '{}', max index: '{}'",
                        col,
                        input.types.len() - 1
                    )));
                }
                PhysicalScalarExpression::Column(col)
            }
            LogicalExpression::Literal(lit) => PhysicalScalarExpression::Literal(lit),
            // LogicalExpression::Unary { op, expr } => PhysicalScalarExpression::Unary {
            //     op,
            //     expr: Box::new(Self::try_from_uncorrelated_expr(*expr, t)?),
            // },
            LogicalExpression::Binary { op, left, right } => {
                let left_datatype = left.datatype(input, &[])?;
                let right_datatype = right.datatype(input, &[])?;

                let left = PhysicalScalarExpression::try_from_uncorrelated_expr(*left, input)?;
                let right = PhysicalScalarExpression::try_from_uncorrelated_expr(*right, input)?;

                let scalar_inputs = &[left_datatype, right_datatype];
                let func = op.scalar_function();
                let specialized = func.specialize(scalar_inputs)?;

                PhysicalScalarExpression::ScalarFunction {
                    function: specialized,
                    inputs: vec![left, right],
                }
            }
            LogicalExpression::ScalarFunction { function, inputs } => {
                let datatypes = inputs
                    .iter()
                    .map(|arg| arg.datatype(input, &[]))
                    .collect::<Result<Vec<_>>>()?;

                let inputs = inputs
                    .into_iter()
                    .map(|expr| PhysicalScalarExpression::try_from_uncorrelated_expr(expr, input))
                    .collect::<Result<Vec<_>>>()?;

                let specialized = function.specialize(&datatypes)?;

                PhysicalScalarExpression::ScalarFunction {
                    function: specialized,
                    inputs,
                }
            }
            _ => unimplemented!(),
        })
    }

    /// Evaluate this expression on a batch.
    ///
    /// The number of elements in the resulting array will equal the number of
    /// rows in the input batch.
    pub fn eval(&self, batch: &Batch) -> Result<Arc<Array>> {
        Ok(match self {
            Self::Column(idx) => batch
                .column(*idx)
                .ok_or_else(|| {
                    RayexecError::new(format!(
                        "Tried to get column at index {} in a batch with {} columns",
                        idx,
                        batch.columns().len()
                    ))
                })?
                .clone(),
            Self::Literal(lit) => Arc::new(lit.as_array(batch.num_rows())),
            Self::ScalarFunction { function, inputs } => {
                let inputs = inputs
                    .iter()
                    .map(|input| input.eval(batch))
                    .collect::<Result<Vec<_>>>()?;
                let refs: Vec<_> = inputs.iter().map(|a| a).collect(); // Can I not?
                let out = (function.function_impl())(&refs)?;

                // TODO: Do we want to Arc here? Should we allow batches to be mutable?

                Arc::new(out)
            }
            _ => unimplemented!(),
        })
    }
}
