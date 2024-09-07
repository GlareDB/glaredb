pub mod aggregate_expr;
pub mod arith_expr;
pub mod between_expr;
pub mod case_expr;
pub mod cast_expr;
pub mod column_expr;
pub mod comparison_expr;
pub mod conjunction_expr;
pub mod is_expr;
pub mod literal_expr;
pub mod negate_expr;
pub mod scalar;
pub mod scalar_function_expr;
pub mod subquery_expr;
pub mod window_expr;

pub mod physical;

use crate::functions::scalar::ScalarFunction;
use crate::logical::binder::bind_context::BindContext;
use aggregate_expr::AggregateExpr;
use arith_expr::ArithExpr;
use between_expr::BetweenExpr;
use case_expr::CaseExpr;
use cast_expr::CastExpr;
use column_expr::ColumnExpr;
use comparison_expr::ComparisonExpr;
use conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use is_expr::IsExpr;
use literal_expr::LiteralExpr;
use negate_expr::NegateExpr;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{not_implemented, RayexecError, Result};
use scalar_function_expr::ScalarFunctionExpr;
use std::fmt::{self, Debug};
use subquery_expr::SubqueryExpr;
use window_expr::WindowExpr;

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Aggregate(AggregateExpr),
    Arith(ArithExpr),
    Between(BetweenExpr),
    Case(CaseExpr),
    Cast(CastExpr),
    Column(ColumnExpr),
    Comparison(ComparisonExpr),
    Conjunction(ConjunctionExpr),
    Is(IsExpr),
    Literal(LiteralExpr),
    Negate(NegateExpr),
    ScalarFunction(ScalarFunctionExpr),
    Subquery(SubqueryExpr),
    Window(WindowExpr),
}

impl Expression {
    pub fn datatype(&self, bind_context: &BindContext) -> Result<DataType> {
        Ok(match self {
            Self::Aggregate(expr) => expr.agg.return_type(),
            Self::Arith(expr) => {
                let func = expr
                    .op
                    .as_scalar_function()
                    .plan_from_expressions(bind_context, &[&expr.left, &expr.right])?;
                func.return_type()
            }
            Self::Between(_) => DataType::Boolean,
            Self::Case(expr) => expr.datatype(bind_context)?,
            Self::Cast(expr) => expr.to.clone(),
            Self::Column(expr) => expr.datatype(bind_context)?,
            Self::Comparison(_) => DataType::Boolean,
            Self::Conjunction(_) => DataType::Boolean,
            Self::Is(_) => DataType::Boolean,
            Self::Literal(expr) => expr.literal.datatype(),
            Self::Negate(expr) => expr.datatype(bind_context)?,
            Self::ScalarFunction(expr) => expr.function.return_type(),
            Self::Subquery(expr) => expr.return_type.clone(),
            Self::Window(_) => not_implemented!("WINDOW"),
        })
    }

    /// ANDs all expressions, only returning None if iterator contains no
    /// expressions.
    pub fn and_all(exprs: impl IntoIterator<Item = Expression>) -> Option<Expression> {
        let mut exprs = exprs.into_iter();
        let left = exprs.next()?;

        Some(exprs.fold(left, |left, right| {
            Expression::Conjunction(ConjunctionExpr {
                left: Box::new(left),
                right: Box::new(right),
                op: ConjunctionOperator::And,
            })
        }))
    }

    pub fn for_each_child_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        match self {
            Self::Aggregate(agg) => {
                for expr in &mut agg.inputs {
                    func(expr)?;
                }
                if let Some(filter) = agg.filter.as_mut() {
                    func(filter)?;
                }
            }
            Self::Arith(arith) => {
                func(&mut arith.left)?;
                func(&mut arith.right)?;
            }
            Self::Between(between) => {
                func(&mut between.lower)?;
                func(&mut between.upper)?;
                func(&mut between.input)?;
            }
            Self::Cast(cast) => {
                func(&mut cast.expr)?;
            }
            Self::Case(case) => {
                for when_then in &mut case.cases {
                    func(&mut when_then.when)?;
                    func(&mut when_then.then)?;
                }
                if let Some(else_expr) = case.else_expr.as_mut() {
                    func(else_expr)?;
                }
            }
            Self::Column(_) => (),
            Self::Comparison(comp) => {
                func(&mut comp.left)?;
                func(&mut comp.right)?;
            }
            Self::Conjunction(conj) => {
                func(&mut conj.left)?;
                func(&mut conj.right)?;
            }
            Self::Is(is) => func(&mut is.input)?,
            Self::Literal(_) => (),
            Self::Negate(negate) => func(&mut negate.expr)?,
            Self::ScalarFunction(scalar) => {
                for input in &mut scalar.inputs {
                    func(input)?;
                }
            }
            Self::Subquery(_) => (),
            Self::Window(window) => {
                for input in &mut window.inputs {
                    func(input)?;
                }
                func(&mut window.filter)?;
                for partition in &mut window.partition_by {
                    func(partition)?;
                }
            }
        }
        Ok(())
    }

    pub fn for_each_child<F>(&self, func: &mut F) -> Result<()>
    where
        F: FnMut(&Expression) -> Result<()>,
    {
        match self {
            Self::Aggregate(agg) => {
                for expr in &agg.inputs {
                    func(expr)?;
                }
                if let Some(filter) = agg.filter.as_ref() {
                    func(filter)?;
                }
            }
            Self::Arith(arith) => {
                func(&arith.left)?;
                func(&arith.right)?;
            }
            Self::Between(between) => {
                func(&between.lower)?;
                func(&between.upper)?;
                func(&between.input)?;
            }
            Self::Cast(cast) => {
                func(&cast.expr)?;
            }
            Self::Case(case) => {
                for when_then in &case.cases {
                    func(&when_then.when)?;
                    func(&when_then.then)?;
                }
                if let Some(else_expr) = case.else_expr.as_ref() {
                    func(else_expr)?;
                }
            }
            Self::Column(_) => (),
            Self::Comparison(comp) => {
                func(&comp.left)?;
                func(&comp.right)?;
            }
            Self::Conjunction(conj) => {
                func(&conj.left)?;
                func(&conj.right)?;
            }
            Self::Is(is) => func(&is.input)?,
            Self::Literal(_) => (),
            Self::Negate(negate) => func(&negate.expr)?,
            Self::ScalarFunction(scalar) => {
                for input in &scalar.inputs {
                    func(input)?;
                }
            }
            Self::Subquery(_) => (),
            Self::Window(window) => {
                for input in &window.inputs {
                    func(input)?;
                }
                func(&window.filter)?;
                for partition in &window.partition_by {
                    func(partition)?;
                }
            }
        }
        Ok(())
    }

    pub fn contains_subquery(&self) -> bool {
        match self {
            Self::Subquery(_) => true,
            _ => {
                let mut has_subquery = false;
                self.for_each_child(&mut |expr| {
                    if has_subquery {
                        return Ok(());
                    }
                    has_subquery = has_subquery || expr.contains_subquery();
                    Ok(())
                })
                .expect("subquery check to no fail");
                has_subquery
            }
        }
    }

    pub fn is_constant(&self) -> bool {
        match self {
            Self::Literal(_) => true,
            _ => {
                let mut is_constant = true;
                self.for_each_child(&mut |expr| {
                    if !is_constant {
                        return Ok(());
                    }
                    is_constant = is_constant && expr.is_constant();
                    Ok(())
                })
                .expect("constant check to to not fail");
                is_constant
            }
        }
    }

    /// Walks the expression to ensure it contains only a single logical column.
    ///
    /// Multiple column expressions may exist, but they must point to the same column.
    pub fn contains_single_column(&self) -> bool {
        fn inner(expr: &Expression, current: &mut Option<ColumnExpr>) -> bool {
            match expr {
                Expression::Column(col) => {
                    if let Some(curr) = current {
                        return curr == col;
                    }
                    *current = Some(*col);
                    true
                }
                other => {
                    let mut result = true;
                    other
                        .for_each_child(&mut |expr| {
                            result = result && inner(expr, current);
                            Ok(())
                        })
                        .expect("not to fail");
                    result
                }
            }
        }

        let mut found = None;
        inner(self, &mut found)
    }

    pub const fn is_column_expr(&self) -> bool {
        matches!(self, Self::Column(_))
    }

    /// Try to get a top-level literal from this expression, erroring if it's
    /// not one.
    pub fn try_into_scalar(self) -> Result<OwnedScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit.literal),
            other => Err(RayexecError::new(format!("Not a literal: {other:?}"))),
        }
    }
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Aggregate(expr) => write!(f, "{}", expr),
            Self::Arith(expr) => write!(f, "{}", expr),
            Self::Between(expr) => write!(f, "{}", expr),
            Self::Case(expr) => write!(f, "{}", expr),
            Self::Cast(expr) => write!(f, "{}", expr),
            Self::Column(expr) => write!(f, "{}", expr),
            Self::Comparison(expr) => write!(f, "{}", expr),
            Self::Conjunction(expr) => write!(f, "{}", expr),
            Self::Is(expr) => write!(f, "{}", expr),
            Self::Literal(expr) => write!(f, "{}", expr),
            Self::Negate(expr) => write!(f, "{}", expr),
            Self::ScalarFunction(expr) => write!(f, "{}", expr),
            Self::Subquery(expr) => write!(f, "{}", expr),
            Self::Window(expr) => write!(f, "{}", expr),
        }
    }
}

pub trait AsScalarFunction {
    /// Returns the scalar function that implements the expression.
    fn as_scalar_function(&self) -> &dyn ScalarFunction;
}

impl<S: ScalarFunction> AsScalarFunction for S {
    fn as_scalar_function(&self) -> &dyn ScalarFunction {
        self as _
    }
}
