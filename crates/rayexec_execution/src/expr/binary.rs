use arrow::error::ArrowError;
use arrow_array::{new_empty_array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_parser::ast;
use std::fmt;
use std::hash::Hash;
use std::sync::Arc;

use crate::types::batch::{DataBatch, DataBatchSchema};

use super::{Expression, PhysicalExpr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinaryOperator {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    And,
    Or,
}

impl TryFrom<ast::BinaryOperator> for BinaryOperator {
    type Error = RayexecError;
    fn try_from(value: ast::BinaryOperator) -> Result<Self> {
        Ok(match value {
            ast::BinaryOperator::Plus => BinaryOperator::Plus,
            ast::BinaryOperator::Minus => BinaryOperator::Minus,
            ast::BinaryOperator::Multiply => BinaryOperator::Multiply,
            ast::BinaryOperator::Divide => BinaryOperator::Divide,
            ast::BinaryOperator::Modulo => BinaryOperator::Modulo,
            ast::BinaryOperator::Eq => BinaryOperator::Eq,
            ast::BinaryOperator::NotEq => BinaryOperator::NotEq,
            ast::BinaryOperator::Gt => BinaryOperator::Gt,
            ast::BinaryOperator::GtEq => BinaryOperator::GtEq,
            ast::BinaryOperator::Lt => BinaryOperator::Lt,
            ast::BinaryOperator::LtEq => BinaryOperator::LtEq,
            ast::BinaryOperator::And => BinaryOperator::And,
            ast::BinaryOperator::Or => BinaryOperator::Or,
            other => {
                return Err(RayexecError::new(format!(
                    "Unsupported SQL operator: {other:?}"
                )))
            }
        })
    }
}

impl fmt::Display for BinaryOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                BinaryOperator::Eq => "=",
                BinaryOperator::NotEq => "!=",
                BinaryOperator::Lt => "<",
                BinaryOperator::LtEq => "<=",
                BinaryOperator::Gt => ">",
                BinaryOperator::GtEq => ">=",
                BinaryOperator::Plus => "+",
                BinaryOperator::Minus => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                BinaryOperator::Modulo => "%",
                BinaryOperator::And => "AND",
                BinaryOperator::Or => "OR",
            }
        )
    }
}

#[derive(Debug)]
pub struct BinaryExpr {
    pub left: Box<Expression>,
    pub op: BinaryOperator,
    pub right: Box<Expression>,
}

#[derive(Debug)]
pub struct PhysicalBinaryExpr {
    pub left: Box<dyn PhysicalExpr>,
    pub op: BinaryOperator,
    pub right: Box<dyn PhysicalExpr>,
}

impl PhysicalBinaryExpr {
    fn eval_inner(&self, left: ArrayRef, right: ArrayRef) -> Result<ArrayRef, ArrowError> {
        let arr = match self.op {
            BinaryOperator::Eq => Arc::new(arrow::compute::kernels::cmp::eq(&left, &right)?),
            BinaryOperator::NotEq => Arc::new(arrow::compute::kernels::cmp::neq(&left, &right)?),
            BinaryOperator::Lt => Arc::new(arrow::compute::kernels::cmp::lt(&left, &right)?),
            BinaryOperator::LtEq => Arc::new(arrow::compute::kernels::cmp::lt_eq(&left, &right)?),
            BinaryOperator::Gt => Arc::new(arrow::compute::kernels::cmp::gt(&left, &right)?),
            BinaryOperator::GtEq => Arc::new(arrow::compute::kernels::cmp::gt_eq(&left, &right)?),
            BinaryOperator::Plus => arrow::compute::kernels::numeric::add(&left, &right)?,
            BinaryOperator::Minus => arrow::compute::kernels::numeric::sub(&left, &right)?,
            BinaryOperator::Multiply => arrow::compute::kernels::numeric::mul(&left, &right)?,
            BinaryOperator::Divide => arrow::compute::kernels::numeric::div(&left, &right)?,
            BinaryOperator::Modulo => arrow::compute::kernels::numeric::rem(&left, &right)?,
            _ => unimplemented!(),
        };

        Ok(arr)
    }
}

impl fmt::Display for PhysicalBinaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl PhysicalExpr for PhysicalBinaryExpr {
    fn data_type(&self, input: &DataBatchSchema) -> Result<DataType> {
        match self.op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::And
            | BinaryOperator::Or => Ok(DataType::Boolean),
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Modulo => {
                let left_dt = self.left.data_type(input)?;
                let right_dt = self.right.data_type(input)?;

                let left = new_empty_array(&left_dt);
                let right = new_empty_array(&right_dt);

                let result = match self.op {
                    BinaryOperator::Plus => arrow::compute::kernels::numeric::add(&left, &right),
                    BinaryOperator::Minus => arrow::compute::kernels::numeric::sub(&left, &right),
                    BinaryOperator::Multiply => {
                        arrow::compute::kernels::numeric::mul(&left, &right)
                    }
                    BinaryOperator::Divide => arrow::compute::kernels::numeric::div(&left, &right),
                    BinaryOperator::Modulo => arrow::compute::kernels::numeric::rem(&left, &right),
                    _ => unreachable!(),
                };

                // TODO: Try coerce on error.
                let arr = result.context("arrow kernel")?;

                Ok(arr.data_type().clone())
            }
        }
    }

    fn nullable(&self, input: &DataBatchSchema) -> Result<bool> {
        Ok(self.left.nullable(input)? || self.right.nullable(input)?)
    }

    fn eval(&self, batch: &DataBatch) -> Result<ArrayRef> {
        let left = self.left.eval(batch)?;
        let right = self.right.eval(batch)?;
        let arr = self
            .eval_inner(left, right)
            .context("eval binary expression")?;

        Ok(arr)
    }
}
