use crate::errors::Result;
use arrow_array::{new_empty_array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema};
use std::fmt;
use std::sync::Arc;

use super::{logical::LogicalExpr, PhysicalExpr};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
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

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Operator::Eq => "=",
                Operator::NotEq => "!=",
                Operator::Lt => "<",
                Operator::LtEq => "<=",
                Operator::Gt => ">",
                Operator::GtEq => ">=",
                Operator::Plus => "+",
                Operator::Minus => "-",
                Operator::Multiply => "*",
                Operator::Divide => "/",
                Operator::Modulo => "%",
                Operator::And => "AND",
                Operator::Or => "OR",
            }
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BinaryExpr {
    pub left: Box<LogicalExpr>,
    pub op: Operator,
    pub right: Box<LogicalExpr>,
}

#[derive(Debug)]
pub struct PhysicalBinaryExpr {
    pub left: Box<dyn PhysicalExpr>,
    pub op: Operator,
    pub right: Box<dyn PhysicalExpr>,
}

impl fmt::Display for PhysicalBinaryExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.op, self.right)
    }
}

impl PhysicalExpr for PhysicalBinaryExpr {
    fn data_type(&self, input: &Schema) -> Result<DataType> {
        match self.op {
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq
            | Operator::And
            | Operator::Or => Ok(DataType::Boolean),
            Operator::Plus
            | Operator::Minus
            | Operator::Multiply
            | Operator::Divide
            | Operator::Modulo => {
                let left_dt = self.left.data_type(input)?;
                let right_dt = self.right.data_type(input)?;

                let left = new_empty_array(&left_dt);
                let right = new_empty_array(&right_dt);

                let result = match self.op {
                    Operator::Plus => arrow::compute::kernels::numeric::add(&left, &right),
                    Operator::Minus => arrow::compute::kernels::numeric::sub(&left, &right),
                    Operator::Multiply => arrow::compute::kernels::numeric::mul(&left, &right),
                    Operator::Divide => arrow::compute::kernels::numeric::div(&left, &right),
                    Operator::Modulo => arrow::compute::kernels::numeric::rem(&left, &right),
                    _ => unreachable!(),
                };

                // TODO: Try coerce on error.
                let arr = result?;

                Ok(arr.data_type().clone())
            }
        }
    }

    fn nullable(&self, input: &Schema) -> Result<bool> {
        Ok(self.left.nullable(input)? || self.right.nullable(input)?)
    }

    fn eval(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = self.left.eval(batch)?;
        let right = self.right.eval(batch)?;

        let arr = match self.op {
            Operator::Eq => Arc::new(arrow::compute::kernels::cmp::eq(&left, &right)?),
            Operator::NotEq => Arc::new(arrow::compute::kernels::cmp::neq(&left, &right)?),
            Operator::Lt => Arc::new(arrow::compute::kernels::cmp::lt(&left, &right)?),
            Operator::LtEq => Arc::new(arrow::compute::kernels::cmp::lt_eq(&left, &right)?),
            Operator::Gt => Arc::new(arrow::compute::kernels::cmp::gt(&left, &right)?),
            Operator::GtEq => Arc::new(arrow::compute::kernels::cmp::gt_eq(&left, &right)?),
            Operator::Plus => arrow::compute::kernels::numeric::add(&left, &right)?,
            Operator::Minus => arrow::compute::kernels::numeric::sub(&left, &right)?,
            Operator::Multiply => arrow::compute::kernels::numeric::mul(&left, &right)?,
            Operator::Divide => arrow::compute::kernels::numeric::div(&left, &right)?,
            Operator::Modulo => arrow::compute::kernels::numeric::rem(&left, &right)?,
            _ => unimplemented!(),
        };

        Ok(arr)
    }
}
