use crate::arrow::chunk::Chunk;
use crate::arrow::column::{compute, Column};
use crate::arrow::datatype::{DataType, GetArrowDataType};
use crate::arrow::scalar::ScalarOwned;
use crate::errors::{internal, LemurError, Result};
use serde::{Deserialize, Serialize};

/// The result of an expression evaluation.
#[derive(Debug)]
pub enum ScalarExprResult {
    Column(Column),
    Scalar(ScalarOwned),
}

impl ScalarExprResult {
    /// Convert self into column.
    ///
    /// If self is a scalar, a Column of size `size` will be produced, otherwise
    /// the column is returned as is with no modifications.
    pub fn into_column_or_expand(self, size: usize) -> Result<Column> {
        match self {
            ScalarExprResult::Column(col) => Ok(col),
            ScalarExprResult::Scalar(scalar) => {
                Column::try_from_scalars(scalar.data_type(), std::iter::repeat(scalar).take(size))
            }
        }
    }

    pub fn try_get_column(&self) -> Option<Column> {
        match self {
            ScalarExprResult::Column(column) => Some(column.clone()),
            ScalarExprResult::Scalar(_) => None,
        }
    }
}

impl From<Column> for ScalarExprResult {
    fn from(col: Column) -> Self {
        ScalarExprResult::Column(col)
    }
}

impl From<ScalarOwned> for ScalarExprResult {
    fn from(scalar: ScalarOwned) -> Self {
        ScalarExprResult::Scalar(scalar)
    }
}

/// An expression tree that is evaluated against columns in a Chunk.
///
/// All expressions produce a single output column.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarExpr {
    /// Reference a column in the input.
    Column(usize),
    /// A constant value.
    Constant(ScalarOwned),
    /// An operation acting on a single column.
    Unary {
        op: UnaryOperation,
        input: Box<ScalarExpr>,
    },
    /// An operation acting on two columns.
    Binary {
        op: BinaryOperation,
        left: Box<ScalarExpr>,
        right: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    /// Evaluate an expression against a chunk.
    pub fn evaluate(&self, chunk: &Chunk) -> Result<ScalarExprResult> {
        Ok(match self {
            ScalarExpr::Column(idx) => chunk
                .get_column(*idx)
                .cloned()
                .ok_or(LemurError::IndexOutOfBounds(*idx))?
                .into(),
            ScalarExpr::Constant(constant) => constant.clone().into(),
            ScalarExpr::Unary { op, input } => op.evaluate(input, chunk)?,
            ScalarExpr::Binary { op, left, right } => op.evaluate(left, right, chunk)?,
        })
    }

    pub fn is_constant(&self) -> bool {
        match self {
            ScalarExpr::Column(_) => false,
            ScalarExpr::Constant(_) => true,
            ScalarExpr::Unary { input, .. } => input.is_constant(),
            ScalarExpr::Binary { left, right, .. } => left.is_constant() && right.is_constant(),
        }
    }

    /// Try to evaluate the expression as a constant expression.
    ///
    /// Note that this will always return a scalar.
    pub fn try_evaluate_constant(&self) -> Result<ScalarOwned> {
        if !self.is_constant() {
            return Err(internal!("expression not constant: {:?}", self));
        }

        let empty = Chunk::empty();
        match self.evaluate(&empty)? {
            ScalarExprResult::Column(_) => Err(internal!("expression evaluation returned column")),
            ScalarExprResult::Scalar(v) => Ok(v),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnaryOperation {
    Not,
    IsNull,
    IsNotNull,
    Cast { to: DataType },
}

impl UnaryOperation {
    fn evaluate(&self, input: &ScalarExpr, chunk: &Chunk) -> Result<ScalarExprResult> {
        let column = match input.evaluate(chunk)? {
            ScalarExprResult::Column(col) => col,
            ScalarExprResult::Scalar(scalar) => Column::from(scalar),
        };

        Ok(match self {
            UnaryOperation::Not => column.not()?.into(),
            UnaryOperation::IsNull => column.is_null()?.into(),
            UnaryOperation::IsNotNull => column.is_not_null()?.into(),
            UnaryOperation::Cast { to } => column.cast(*to)?.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    fn evaluate(
        &self,
        left: &ScalarExpr,
        right: &ScalarExpr,
        chunk: &Chunk,
    ) -> Result<ScalarExprResult> {
        let left = left.evaluate(chunk)?;
        let right = right.evaluate(chunk)?;

        let col = match (left, right) {
            (ScalarExprResult::Column(left), ScalarExprResult::Column(right)) => {
                self.evaluate_columns(&left, &right)?
            }
            (ScalarExprResult::Column(left), ScalarExprResult::Scalar(right)) => {
                self.evaluate_column_scalar(&left, &right)?
            }
            (ScalarExprResult::Scalar(left), ScalarExprResult::Column(right)) => {
                self.evaluate_scalar_column(&left, &right)?
            }
            (ScalarExprResult::Scalar(left), ScalarExprResult::Scalar(right)) => {
                // TODO: Avoid allocating a column here.
                self.evaluate_scalar_scalar(&left, &right)?
            }
        };
        Ok(col.into())
    }

    fn evaluate_columns(&self, left: &Column, right: &Column) -> Result<Column> {
        if left.len() != right.len() {
            return Err(internal!(
                "length mismatch between left ({:?}) and right ({:?})",
                left.len(),
                right.len()
            ));
        }

        if left.get_arrow_data_type() != right.get_arrow_data_type() {
            return Err(LemurError::TypeMismatch);
        }

        match self {
            BinaryOperation::Eq => compute::eq(left, right),
            BinaryOperation::Neq => compute::neq(left, right),
            BinaryOperation::LtEq => compute::lt_eq(left, right),
            BinaryOperation::GtEq => compute::gt_eq(left, right),
            BinaryOperation::Gt => compute::gt(left, right),
            BinaryOperation::Lt => compute::lt(left, right),
            BinaryOperation::Add => compute::add(left, right),
            BinaryOperation::Sub => compute::sub(left, right),
            BinaryOperation::Mul => compute::mul(left, right),
            BinaryOperation::Div => compute::div(left, right),
            op => Err(internal!("unsupported op: {:?}", op)),
        }
    }

    fn evaluate_column_scalar(&self, left: &Column, right: &ScalarOwned) -> Result<Column> {
        if left.get_arrow_data_type() != right.get_arrow_data_type() {
            return Err(LemurError::TypeMismatch);
        }

        match self {
            BinaryOperation::Eq => compute::eq_scalar(left, right),
            BinaryOperation::Neq => compute::neq_scalar(left, right),
            BinaryOperation::LtEq => compute::lt_eq_scalar(left, right),
            BinaryOperation::GtEq => compute::gt_eq_scalar(left, right),
            BinaryOperation::Gt => compute::gt_scalar(left, right),
            BinaryOperation::Lt => compute::lt_scalar(left, right),
            BinaryOperation::Add => compute::add_scalar(left, right),
            BinaryOperation::Sub => compute::sub_scalar(left, right),
            BinaryOperation::Mul => compute::mul_scalar(left, right),
            BinaryOperation::Div => compute::div_scalar(left, right),
            op => Err(internal!("unsupported op: {:?}", op)),
        }
    }

    fn evaluate_scalar_column(&self, left: &ScalarOwned, right: &Column) -> Result<Column> {
        if left.get_arrow_data_type() != right.get_arrow_data_type() {
            return Err(LemurError::TypeMismatch);
        }

        match self {
            BinaryOperation::LtEq => compute::gt_scalar(right, left),
            BinaryOperation::GtEq => compute::lt_scalar(right, left),
            BinaryOperation::Gt => compute::lt_eq_scalar(right, left),
            BinaryOperation::Lt => compute::gt_eq_scalar(right, left),
            _ => self.evaluate_column_scalar(right, left),
        }
    }

    fn evaluate_scalar_scalar(&self, left: &ScalarOwned, right: &ScalarOwned) -> Result<Column> {
        if left.get_arrow_data_type() != right.get_arrow_data_type() {
            return Err(LemurError::TypeMismatch);
        }

        // jank
        let left: Column = left.clone().into();
        self.evaluate_column_scalar(&left, right)
    }
}
