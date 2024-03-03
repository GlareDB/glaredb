pub mod binary;
pub mod literal;
pub mod scalar;

use self::scalar::{BinaryOperator, ScalarValue, UnaryOperator, VariadicOperator};
use crate::{
    planner::{
        operator::{LogicalExpression},
    },
    types::batch::{DataBatch},
};
use arrow_array::{ArrayRef, BooleanArray};

use rayexec_error::{RayexecError, Result};
use std::fmt::{Debug};

#[derive(Debug)]
pub enum Expression {
    Literal(ScalarValue),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalScalarExpression {
    /// Reference to a column in the input batch.
    Column(usize),
    /// A scalar literal.
    Literal(ScalarValue),
    /// Unary function.
    Unary {
        op: UnaryOperator,
        expr: Box<PhysicalScalarExpression>,
    },
    /// Binary function.
    Binary {
        op: BinaryOperator,
        left: Box<PhysicalScalarExpression>,
        right: Box<PhysicalScalarExpression>,
    },
    /// Variadic function.
    Variadic {
        op: VariadicOperator,
        exprs: Vec<PhysicalScalarExpression>,
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
    pub fn try_from_uncorrelated_expr(logical: LogicalExpression) -> Result<Self> {
        Ok(match logical {
            LogicalExpression::ColumnRef(col) => {
                PhysicalScalarExpression::Column(col.try_as_uncorrelated()?)
            }
            LogicalExpression::Literal(lit) => PhysicalScalarExpression::Literal(lit),
            LogicalExpression::Unary { op, expr } => PhysicalScalarExpression::Unary {
                op,
                expr: Box::new(Self::try_from_uncorrelated_expr(*expr)?),
            },
            LogicalExpression::Binary { op, left, right } => PhysicalScalarExpression::Binary {
                op,
                left: Box::new(Self::try_from_uncorrelated_expr(*left)?),
                right: Box::new(Self::try_from_uncorrelated_expr(*right)?),
            },
            _ => unimplemented!(),
        })
    }

    /// Evaluate this expression on a batch.
    ///
    /// The number of elements in the resulting array will equal the number of
    /// rows in the input batch.
    pub fn eval(&self, batch: &DataBatch) -> Result<ArrayRef> {
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
            Self::Literal(lit) => lit.as_array(batch.num_rows())?,
            Self::Binary { op, left, right } => {
                let left = left.eval(batch)?;
                let right = right.eval(batch)?;
                op.eval(&left, &right)?
            }
            _ => unimplemented!(),
        })
    }

    /// Evaluate this expression on a batch where selection is true.
    pub fn eval_selection(&self, _batch: &DataBatch, _selection: &BooleanArray) -> Result<ArrayRef> {
        unimplemented!()
    }
}
