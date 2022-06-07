use crate::datatype::{DataType, DataValue, NullableType, RelationSchema};
use crate::column::{NullableColumnVec, BoolVec};
use fmtutil::DisplaySlice;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, thiserror::Error)]
pub enum ExprError {
    #[error("type mismatch: have: {have}, want: {want}")]
    TypeMismatch {
        have: NullableType,
        want: NullableType,
    },
    #[error("type not numeric: {0}")]
    NotNumeric(NullableType),
    #[error("missing column index {0} in provided relation")]
    MissingColumn(usize),
}

/// Scalar expressions that work on columns at a time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarExpr {
    /// Pick a column from the input relation.
    Column(usize),
    /// A constant value.
    Constant(DataValue, NullableType),
    Unary {
        operation: UnaryOperation,
        expr: Box<ScalarExpr>,
    },
    Binary {
        operation: BinaryOperation,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
    /// Cast the output of an expression to another type.
    Cast {
        expr: Box<ScalarExpr>,
        datatype: NullableType,
    },
}

impl ScalarExpr {
    /// Given an input with some schema, compute the output type of the
    /// expression.
    pub fn output_type(&self, input: &RelationSchema) -> Result<NullableType, ExprError> {
        Ok(match self {
            Self::Column(idx) => input
                .columns
                .get(*idx)
                .cloned()
                .ok_or(ExprError::MissingColumn(*idx))?,
            Self::Constant(_, datatype) => datatype.clone(),
            Self::Unary { operation, expr } => {
                let expr_type = expr.output_type(input)?;
                operation.output_type(&expr_type)
            }
            Self::Binary {
                operation,
                left,
                right,
            } => {
                let left = left.output_type(input)?;
                let right = right.output_type(input)?;
                operation.output_type(&left, &right)?
            }
            Self::Cast { datatype, .. } => datatype.clone(),
        })
    }
}

impl fmt::Display for ScalarExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarExpr::Column(idx) => write!(f, "#{}", idx),
            ScalarExpr::Constant(val, typ) => write!(f, "{} ({})", val, typ),
            ScalarExpr::Unary { operation, expr } => write!(f, "{}({})", operation, expr),
            ScalarExpr::Binary {
                operation,
                left,
                right,
            } => write!(f, "{}({}, {})", operation, left, right),
            ScalarExpr::Cast { expr, datatype } => write!(f, "cast({} as {})", expr, datatype),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperation {
    IsNull,
    IsNotNull,
}

impl UnaryOperation {
    /// Given an input, determine what the output type is.
    fn output_type(&self, input_type: &NullableType) -> NullableType {
        match self {
            Self::IsNull => NullableType {
                datatype: DataType::Bool,
                nullable: true,
            },
            Self::IsNotNull => NullableType {
                datatype: DataType::Bool,
                nullable: true,
            },
        }
    }
}

impl fmt::Display for UnaryOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOperation::IsNull => write!(f, "is_null"),
            UnaryOperation::IsNotNull => write!(f, "is_not_null"),
        }
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
    fn output_type(
        &self,
        left_type: &NullableType,
        right_type: &NullableType,
    ) -> Result<NullableType, ExprError> {
        Ok(match self {
            Self::Eq
            | Self::Neq
            | Self::LtEq
            | Self::GtEq
            | Self::Gt
            | Self::Lt
            | Self::And
            | Self::Or
            | Self::Like => NullableType {
                datatype: DataType::Bool,
                nullable: true,
            },
            Self::Add | Self::Sub | Self::Mul | Self::Div => {
                if !left_type.is_numeric() {
                    return Err(ExprError::NotNumeric(left_type.clone()));
                }
                // TODO: Try promoting/casting types as necessary. Once this is
                // added in, that logic will need to be shared with whatever
                // actually evaluates the expression.
                if left_type != right_type {
                    return Err(ExprError::TypeMismatch {
                        have: left_type.clone(),
                        want: right_type.clone(),
                    });
                }

                left_type.clone()
            }
        })
    }
}

impl fmt::Display for BinaryOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BinaryOperation::Eq => write!(f, "eq"),
            BinaryOperation::Neq => write!(f, "neq"),
            BinaryOperation::LtEq => write!(f, "lt_eq"),
            BinaryOperation::GtEq => write!(f, "gt_eq"),
            BinaryOperation::Lt => write!(f, "lt"),
            BinaryOperation::Gt => write!(f, "gt"),
            BinaryOperation::And => write!(f, "and"),
            BinaryOperation::Or => write!(f, "or"),
            BinaryOperation::Like => write!(f, "like"),
            BinaryOperation::Add => write!(f, "add"),
            BinaryOperation::Sub => write!(f, "sub"),
            BinaryOperation::Mul => write!(f, "mul"),
            BinaryOperation::Div => write!(f, "div"),
        }
    }
}
