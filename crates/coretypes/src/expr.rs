use crate::datatype::{DataType, DataValue, NullableType, RelationSchema};
use crate::column::{NullableColumnVec, BoolVec};
use std::fmt;

/// Scalar expressions that work on columns at a time.
#[derive(Debug, Clone)]
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
}

impl ScalarExpr {
    /// Given an input with some schema, compute the output type of the expression.
    pub fn output_type(&self, input: &RelationSchema) -> Option<NullableType> {
        match self {
            Self::Column(idx) => input.columns.get(*idx).cloned(),
            Self::Constant(_, datatype) => Some(datatype.clone()),
            Self::Unary { operation, expr } => {
                let expr_type = expr.output_type(input)?;
                Some(operation.output_type(&expr_type))
            }
            Self::Binary {
                operation,
                left,
                right,
            } => {
                let left = left.output_type(input)?;
                let right = right.output_type(input)?;
                Some(operation.output_type(&left, &right))
            }
            _ => None,
        }
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
        }
    }
}

#[derive(Debug, Clone)]
pub enum UnaryOperation {
    IsNull,
    IsNotNull,
}

impl UnaryOperation {
    /// Given an input, determine what the output type is.
    pub fn output_type(&self, input_type: &NullableType) -> NullableType {
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

#[derive(Debug, Clone)]
pub enum BinaryOperation {}

impl BinaryOperation {
    pub fn output_type(&self, left_type: &NullableType, right_type: &NullableType) -> NullableType {
        unimplemented!()
    }
}

impl fmt::Display for BinaryOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        unimplemented!()
    }
}

fn is_null(input: &NullableColumnVec) -> NullableColumnVec {
    let mut out = BoolVec::with_capacity(input.len());
    let validity = input.get_validity();
    for val in validity.iter().by_refs() {
        out.copy_push(val);
    }
    NullableColumnVec::new_all_valid(out.into())
}
