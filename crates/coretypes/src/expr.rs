use crate::batch::{Batch, BatchRepr};
use crate::column::{BoolVec, ColumnVec, NullableColumnVec, SqlEq};
use crate::datatype::{DataType, DataValue, NullableType, RelationSchema};
use anyhow::anyhow;
use fmtutil::DisplaySlice;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

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
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
}

/// The result of an expression evaluation.
#[derive(Debug, Clone)]
pub enum EvaluatedExpr {
    ColumnRef(Arc<NullableColumnVec>),
    Column(NullableColumnVec),
    Value(DataValue),
}

impl EvaluatedExpr {
    pub fn try_get_bool_vec(&self) -> Option<&BoolVec> {
        match self {
            EvaluatedExpr::ColumnRef(col) => col.get_values().try_as_bool_vec(),
            EvaluatedExpr::Column(col) => col.get_values().try_as_bool_vec(),
            EvaluatedExpr::Value(_) => None,
        }
    }

    pub fn try_into_arc_vec(self) -> Option<Arc<NullableColumnVec>> {
        match self {
            EvaluatedExpr::ColumnRef(col) => Some(col),
            EvaluatedExpr::Column(col) => Some(Arc::new(col)),
            EvaluatedExpr::Value(_) => None,
        }
    }

    fn try_get_column(&self) -> Option<&NullableColumnVec> {
        match self {
            EvaluatedExpr::ColumnRef(col) => Some(col),
            EvaluatedExpr::Column(col) => Some(col),
            EvaluatedExpr::Value(_) => None,
        }
    }
}

impl From<Arc<NullableColumnVec>> for EvaluatedExpr {
    fn from(v: Arc<NullableColumnVec>) -> Self {
        EvaluatedExpr::ColumnRef(v)
    }
}

impl From<NullableColumnVec> for EvaluatedExpr {
    fn from(v: NullableColumnVec) -> Self {
        Self::Column(v)
    }
}

impl From<DataValue> for EvaluatedExpr {
    fn from(v: DataValue) -> Self {
        Self::Value(v)
    }
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

    /// Evaluate an expression on an input batch.
    pub fn evaluate(&self, input: &BatchRepr) -> Result<EvaluatedExpr, ExprError> {
        Ok(match self {
            Self::Column(idx) => input
                .get_batch()
                .get_column(*idx)
                .cloned()
                .ok_or(ExprError::MissingColumn(*idx))?
                .into(),
            Self::Constant(value, _) => value.clone().into(),
            Self::Binary {
                operation,
                left,
                right,
            } => operation.evaluate(left, right, input)?,
            _ => unimplemented!(),
        })
    }

    /// Evaluate an expression that produces a constant.
    pub fn evaluate_constant(&self) -> Result<DataValue, ExprError> {
        Ok(match self {
            Self::Constant(value, _) => value.clone(),
            _ => unimplemented!(),
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

    fn evaluate(
        &self,
        left: &ScalarExpr,
        right: &ScalarExpr,
        input: &BatchRepr,
    ) -> Result<EvaluatedExpr, ExprError> {
        let left_evaled = left.evaluate(input)?;
        let right_evaled = right.evaluate(input)?;
        // TODO: Handle single values in evaluated expressions as well.
        Ok(match self {
            Self::Eq => {
                let left_col = left_evaled
                    .try_get_column()
                    .ok_or(anyhow!("left not a column"))?;
                let right_col = right_evaled
                    .try_get_column()
                    .ok_or(anyhow!("right not a column"))?;
                let out = left_col.sql_eq(right_col).into();
                NullableColumnVec::new_all_valid(out).into()
            }
            _ => unimplemented!(),
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
