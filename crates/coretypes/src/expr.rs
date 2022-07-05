use crate::batch::{Batch, BatchRepr};
use crate::datatype::{DataType, DataValue, NullableType, RelationSchema};
use crate::vec::{compute::*, BoolVec, ColumnVec};
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
///
/// Results may either be represented as a column or a single value.
#[derive(Debug, Clone, PartialEq)]
pub enum EvaluatedExpr {
    ColumnRef(Arc<ColumnVec>),
    Column(ColumnVec),
    /// Expression produced a single value (e.g. aggregate). Type is included so
    /// that we know what to with with null values.
    Value(DataValue, DataType),
}

impl EvaluatedExpr {
    pub fn into_arc_vec(self) -> Arc<ColumnVec> {
        match self {
            EvaluatedExpr::ColumnRef(col) => col,
            EvaluatedExpr::Column(col) => Arc::new(col),
            EvaluatedExpr::Value(value, datatype) => Arc::new(ColumnVec::one(&value, &datatype)),
        }
    }
}

impl From<Arc<ColumnVec>> for EvaluatedExpr {
    fn from(v: Arc<ColumnVec>) -> Self {
        EvaluatedExpr::ColumnRef(v)
    }
}

impl From<ColumnVec> for EvaluatedExpr {
    fn from(v: ColumnVec) -> Self {
        Self::Column(v)
    }
}

// Logic operators on columnvecs produces boolean vectors. Having this From impl
// makes things convenient.
impl From<BoolVec> for EvaluatedExpr {
    fn from(v: BoolVec) -> Self {
        let vec = ColumnVec::from(v);
        vec.into()
    }
}

impl From<(DataValue, DataType)> for EvaluatedExpr {
    fn from((value, datatype): (DataValue, DataType)) -> Self {
        Self::Value(value, datatype)
    }
}

// Logic operators on two boolean values produce a bool value. This From is a
// convenience for that case.
impl From<(bool, DataType)> for EvaluatedExpr {
    fn from((v, _): (bool, DataType)) -> Self {
        Self::Value(v.into(), DataType::Bool)
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
    Aggregate {
        operation: AggregateOperation,
        inner: Box<ScalarExpr>,
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
            Self::Aggregate { operation, inner } => {
                let inner = inner.output_type(input)?;
                operation.output_type(&inner)?
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
            Self::Constant(value, datatype) => {
                EvaluatedExpr::Value(value.clone(), datatype.datatype.clone())
            }
            Self::Binary {
                operation,
                left,
                right,
            } => operation.evaluate(left, right, input)?,
            Self::Aggregate { operation, inner } => operation.evaluate(inner, input)?,
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
            ScalarExpr::Aggregate { operation, inner } => write!(f, "{}({})", operation, inner),
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

/// Macro for building a match expression for two evaluation results. `op` is
/// expected to return a `Result`.
///
/// This allows for dispatching to the correct compute implementation depending
/// on the evaluated result variant.
///
/// Not that this does not allow a scalar value on the left and a vector value
/// on the right.
macro_rules! match_binary_evalulated_expr {
    ($left:ident, $right:ident, $op:path) => {{
        use EvaluatedExpr::*;
        let evaled: EvaluatedExpr = match ($left, $right) {
            (ColumnRef(left), ColumnRef(right)) => $op(left.as_ref(), right.as_ref())?.into(),
            (Column(left), ColumnRef(right)) => $op(&left, right.as_ref())?.into(),
            (Column(left), Column(right)) => $op(&left, &right)?.into(),
            (ColumnRef(left), Column(right)) => $op(left.as_ref(), &right)?.into(),
            (Column(left), Value(right, _)) => $op(&left, &right)?.into(),
            (ColumnRef(left), Value(right, _)) => $op(left.as_ref(), &right)?.into(),
            (Value(left, datatype), Value(right, _)) => {
                ($op(&left, &right)?, datatype.clone()).into()
            }
            (left, right) => {
                return Err(ExprError::Anyhow(anyhow!(
                    "unsupported data value on left hand side of expression, left: {:?}, right: {:?}",
                    left,
                    right,
                )))
            }
        };
        evaled
    }};
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
        let left = left.evaluate(input)?;
        let right = right.evaluate(input)?;

        Ok(match self {
            Self::Eq => match_binary_evalulated_expr!(left, right, VecEq::eq),
            Self::And => match_binary_evalulated_expr!(left, right, VecLogic::and),
            Self::Add => match_binary_evalulated_expr!(left, right, VecAdd::add),
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

macro_rules! match_aggregate_evaluated_expr {
    ($evaled:ident, $op: path) => {{
        let value: DataValue = match $evaled {
            EvaluatedExpr::ColumnRef(col) => $op(col.as_ref())?,
            EvaluatedExpr::Column(col) => $op(&col)?,
            EvaluatedExpr::Value(value, datatype) => $op(&value)?,
        };
        value
    }};
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateOperation {
    Count,
    Sum,
    Min,
    Max,
}

impl AggregateOperation {
    fn output_type(&self, inner: &NullableType) -> Result<NullableType, ExprError> {
        Ok(match self {
            AggregateOperation::Count => DataType::Int64.into(),
            AggregateOperation::Sum => DataType::Int64.into(), // TODO: Need a "numeric" type for summing over int64s
            AggregateOperation::Min => inner.clone(),
            AggregateOperation::Max => inner.clone(),
        })
    }

    fn evaluate(&self, inner: &ScalarExpr, input: &BatchRepr) -> Result<EvaluatedExpr, ExprError> {
        let inner = inner.evaluate(input)?;
        let value = match self {
            AggregateOperation::Count => match_aggregate_evaluated_expr!(inner, VecCountAgg::count),
            AggregateOperation::Sum => match_aggregate_evaluated_expr!(inner, VecNumericAgg::sum),
            AggregateOperation::Min => match_aggregate_evaluated_expr!(inner, VecNumericAgg::min),
            AggregateOperation::Max => match_aggregate_evaluated_expr!(inner, VecNumericAgg::max),
        };
        // Only happens on null. Should probably get the datatype determined in
        // `output_type`.
        let datatype = value
            .datatype()
            .ok_or(anyhow!("unable to determine datatype for aggregate"))?;
        Ok((value, datatype).into())
    }
}

impl fmt::Display for AggregateOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateOperation::Count => write!(f, "count"),
            AggregateOperation::Sum => write!(f, "sum"),
            AggregateOperation::Min => write!(f, "min"),
            AggregateOperation::Max => write!(f, "max"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::Batch;
    use crate::vec::native_vec::Int32Vec;

    #[test]
    fn binary_eval_add_col_val() {
        // Sanity checking.
        let vec: ColumnVec = Int32Vec::from_iter_all_valid([1, 2, 3]).into();
        let batch = Batch::from_columns([vec]).unwrap();

        let expr = ScalarExpr::Binary {
            operation: BinaryOperation::Add,
            left: Box::new(ScalarExpr::Column(0)),
            right: Box::new(ScalarExpr::Constant(
                DataValue::Int32(4),
                DataType::Int32.into(),
            )),
        };

        let res = expr.evaluate(&batch.into()).unwrap();
        let got: Vec<_> = res
            .into_arc_vec()
            .try_as_int32_vec()
            .unwrap()
            .iter_values()
            .cloned()
            .collect();
        assert_eq!(vec![5, 6, 7], got);
    }

    #[test]
    fn agg_eval_sum() {
        let vec: ColumnVec = Int32Vec::from_iter_all_valid([1, 2, 3]).into();
        let batch = Batch::from_columns([vec]).unwrap();

        let expr = ScalarExpr::Aggregate {
            operation: AggregateOperation::Sum,
            inner: Box::new(ScalarExpr::Column(0)),
        };

        let res = expr.evaluate(&batch.into()).unwrap();
        assert_eq!(
            EvaluatedExpr::Value(DataValue::Int32(6), DataType::Int32),
            res
        );
    }
}
