use crate::datatype::{DataType, DataValue, NullableType};
use crate::column::{NullableColumnVec, BoolVec};

/// Scalar expressions that work on columns at a time.
#[derive(Debug)]
pub enum ScalarExpr {
    /// Pick a column from the input relation.
    Column(usize),
    /// A constant value.
    Constant(DataValue),
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

#[derive(Debug)]
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

#[derive(Debug)]
pub enum BinaryOperation {}

fn is_null(input: &NullableColumnVec) -> NullableColumnVec {
    let mut out = BoolVec::with_capacity(input.len());
    let validity = input.get_validity();
    for val in validity.iter().by_refs() {
        out.copy_push(val);
    }
    NullableColumnVec::new_all_valid(out.into())
}
