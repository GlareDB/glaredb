use crate::repr::compute::*;
use crate::repr::df::{DataFrame, Schema};
use crate::repr::value::{Value, ValueType, ValueVec};
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub type ExprNode = Box<ScalarExpr>;

/// The result of evaluating an expression.
#[derive(Debug, Clone)]
pub enum ExprVec {
    Ref(Arc<ValueVec>),
    Owned(ValueVec),
}

impl ExprVec {
    fn unwrap_owned(self) -> Option<ValueVec> {
        match self {
            ExprVec::Owned(v) => Some(v),
            _ => None,
        }
    }
}

impl AsRef<ValueVec> for ExprVec {
    fn as_ref(&self) -> &ValueVec {
        match self {
            Self::Ref(v) => v,
            Self::Owned(v) => v,
        }
    }
}

impl From<Arc<ValueVec>> for ExprVec {
    fn from(v: Arc<ValueVec>) -> Self {
        Self::Ref(v)
    }
}

impl From<ValueVec> for ExprVec {
    fn from(v: ValueVec) -> Self {
        Self::Owned(v)
    }
}

/// An expression tree that is evaluated against columns in a dataframe.
///
/// All expressions produce a single output column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarExpr {
    /// Reference a column in the input.
    Column(usize),
    /// A constant value.
    Constant(Value),
    /// An operation acting on a single column.
    Unary { op: UnaryOperation, input: ExprNode },
    /// An operation acting on two columns.
    Binary {
        op: BinaryOperation,
        left: ExprNode,
        right: ExprNode,
    },
}

impl ScalarExpr {
    /// Given an input schema, determine the ouput type.
    pub fn output_type(&self, schema: &Schema) -> Result<ValueType> {
        Ok(match self {
            ScalarExpr::Column(idx) => schema
                .types
                .get(*idx)
                .cloned()
                .ok_or(anyhow!("missing column in input: {}", idx))?,
            ScalarExpr::Constant(v) => v.value_type(),
            ScalarExpr::Unary { op, input } => op.output_type(input, schema)?,
            ScalarExpr::Binary { op, left, right } => op.output_type(left, right, schema)?,
        })
    }

    pub fn boxed(self) -> ExprNode {
        Box::new(self)
    }

    /// Evaluate self on the columns of the given dataframe.
    ///
    /// In the case of constant evaluation which results in a single value, that
    /// value will be extended to be the same size as the input dataframe.
    pub fn evaluate(&self, df: &DataFrame) -> Result<ExprVec> {
        Ok(match self {
            ScalarExpr::Column(idx) => df
                .get_column_ref(*idx)
                .cloned()
                .ok_or(anyhow!("missing column in dataframe: {}", idx))?
                .into(),
            ScalarExpr::Constant(v) => v.repeat(df.num_rows())?.into(),
            ScalarExpr::Unary { op, input } => op.evaluate(input, df)?,
            ScalarExpr::Binary { op, left, right } => op.evaluate(left, right, df)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperation {
    IsNull,
    IsNotNull,
}

impl UnaryOperation {
    pub fn output_type(&self, input: &ScalarExpr, schema: &Schema) -> Result<ValueType> {
        let _input = input.output_type(schema)?;
        Ok(match self {
            UnaryOperation::IsNull | UnaryOperation::IsNotNull => ValueType::Bool,
        })
    }

    pub fn evaluate(&self, input: &ScalarExpr, df: &DataFrame) -> Result<ExprVec> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOperation {
    Eq,
    Neq,
    LtEq,
    GtEq,
    Gt,
    Lt,
    And,
    Or,
    Like,
    Add,
    Sub,
    Mul,
    Div,
}

impl BinaryOperation {
    pub fn output_type(
        &self,
        left: &ScalarExpr,
        right: &ScalarExpr,
        schema: &Schema,
    ) -> Result<ValueType> {
        let left = left.output_type(schema)?;
        let right = right.output_type(schema)?;

        Ok(match self {
            BinaryOperation::Eq
            | BinaryOperation::Neq
            | BinaryOperation::LtEq
            | BinaryOperation::GtEq
            | BinaryOperation::Gt
            | BinaryOperation::Lt
            | BinaryOperation::And
            | BinaryOperation::Or
            | BinaryOperation::Like => ValueType::Bool,
            BinaryOperation::Add
            | BinaryOperation::Sub
            | BinaryOperation::Mul
            | BinaryOperation::Div => {
                if !left.is_numeric() || !right.is_numeric() {
                    return Err(anyhow!(
                        "left and right both need to be numeric, left: {:?}, right: {:?}",
                        left,
                        right
                    ));
                }
                // TODO: Promote/cast as necessary.
                if left != right {
                    return Err(anyhow!(
                        "type mismatch between left and right, left: {:?}, right: {:?}",
                        left,
                        right
                    ));
                }

                left.clone()
            }
        })
    }

    pub fn evaluate(
        &self,
        left: &ScalarExpr,
        right: &ScalarExpr,
        df: &DataFrame,
    ) -> Result<ExprVec> {
        let left = left.evaluate(df)?;
        let left = left.as_ref();
        let right = right.evaluate(df)?;
        let right = right.as_ref();

        Ok(match self {
            BinaryOperation::Eq => ValueVec::from(VecCmp::eq(left, right)?).into(),
            BinaryOperation::Neq => ValueVec::from(VecCmp::neq(left, right)?).into(),
            BinaryOperation::LtEq => ValueVec::from(VecCmp::le(left, right)?).into(),
            BinaryOperation::GtEq => ValueVec::from(VecCmp::ge(left, right)?).into(),
            BinaryOperation::Lt => ValueVec::from(VecCmp::lt(left, right)?).into(),
            BinaryOperation::Gt => ValueVec::from(VecCmp::gt(left, right)?).into(),
            BinaryOperation::And => ValueVec::from(VecBinaryLogic::and(left, right)?).into(),
            BinaryOperation::Or => ValueVec::from(VecBinaryLogic::or(left, right)?).into(),
            BinaryOperation::Like => todo!(),
            BinaryOperation::Add => VecArith::add(left, right)?.into(),
            BinaryOperation::Sub => VecArith::sub(left, right)?.into(),
            BinaryOperation::Mul => VecArith::mul(left, right)?.into(),
            BinaryOperation::Div => VecArith::div(left, right)?.into(),
        })
    }
}
