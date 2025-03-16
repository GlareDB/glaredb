pub mod evaluator;
pub mod planner;
pub mod selection_evaluator;

pub mod case_expr;
pub mod cast_expr;
pub mod column_expr;
pub mod literal_expr;
pub mod scalar_function_expr;

use std::fmt;

use case_expr::PhysicalCaseExpr;
use cast_expr::PhysicalCastExpr;
use column_expr::PhysicalColumnExpr;
use evaluator::ExpressionState;
use literal_expr::PhysicalLiteralExpr;
use glaredb_error::Result;
use scalar_function_expr::PhysicalScalarFunctionExpr;

use crate::arrays::datatype::DataType;
use crate::functions::aggregate::PlannedAggregateFunction;

#[derive(Debug, Clone)]
pub enum PhysicalScalarExpression {
    Case(PhysicalCaseExpr),
    Cast(PhysicalCastExpr),
    Column(PhysicalColumnExpr),
    Literal(PhysicalLiteralExpr),
    ScalarFunction(PhysicalScalarFunctionExpr),
}

impl From<PhysicalCaseExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalCaseExpr) -> Self {
        PhysicalScalarExpression::Case(value)
    }
}

impl From<PhysicalCastExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalCastExpr) -> Self {
        PhysicalScalarExpression::Cast(value)
    }
}

impl From<PhysicalColumnExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalColumnExpr) -> Self {
        PhysicalScalarExpression::Column(value)
    }
}

impl From<PhysicalLiteralExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalLiteralExpr) -> Self {
        PhysicalScalarExpression::Literal(value)
    }
}

impl From<PhysicalScalarFunctionExpr> for PhysicalScalarExpression {
    fn from(value: PhysicalScalarFunctionExpr) -> Self {
        PhysicalScalarExpression::ScalarFunction(value)
    }
}

impl PhysicalScalarExpression {
    pub(crate) fn create_state(&self, batch_size: usize) -> Result<ExpressionState> {
        match self {
            Self::Case(expr) => expr.create_state(batch_size),
            Self::Cast(expr) => expr.create_state(batch_size),
            Self::Column(expr) => expr.create_state(batch_size),
            Self::Literal(expr) => expr.create_state(batch_size),
            Self::ScalarFunction(expr) => expr.create_state(batch_size),
        }
    }

    pub fn datatype(&self) -> DataType {
        match self {
            Self::Case(expr) => expr.datatype(),
            Self::Cast(expr) => expr.datatype(),
            Self::Column(expr) => expr.datatype(),
            Self::Literal(expr) => expr.datatype(),
            Self::ScalarFunction(expr) => expr.datatype(),
        }
    }
}

impl fmt::Display for PhysicalScalarExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Case(expr) => expr.fmt(f),
            Self::Cast(expr) => expr.fmt(f),
            Self::Column(expr) => expr.fmt(f),
            Self::Literal(expr) => expr.fmt(f),
            Self::ScalarFunction(expr) => expr.fmt(f),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalAggregateExpression {
    /// The function we'll be calling to produce the aggregate states.
    pub function: PlannedAggregateFunction,
    /// Column expressions we're aggregating on.
    pub columns: Vec<PhysicalColumnExpr>,
    /// If inputs are distinct.
    pub is_distinct: bool,
    // TODO: Filter
}

impl PhysicalAggregateExpression {
    pub fn new<C>(function: PlannedAggregateFunction, columns: impl IntoIterator<Item = C>) -> Self
    where
        C: Into<PhysicalColumnExpr>,
    {
        PhysicalAggregateExpression {
            function,
            columns: columns.into_iter().map(|c| c.into()).collect(),
            is_distinct: false,
        }
    }

    pub fn contains_column_idx(&self, column: usize) -> bool {
        self.columns.iter().any(|expr| expr.idx == column)
    }
}

#[derive(Debug, Clone)]
pub struct PhysicalSortExpression {
    /// Column this expression is for.
    pub column: PhysicalScalarExpression,
    /// If sort should be descending.
    pub desc: bool,
    /// If nulls should be ordered first.
    pub nulls_first: bool,
}
