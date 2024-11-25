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
pub mod scalar_function_expr;
pub mod subquery_expr;
pub mod unnest_expr;
pub mod window_expr;

pub mod physical;

use std::collections::HashSet;
use std::fmt::{self, Debug};

use aggregate_expr::AggregateExpr;
use arith_expr::{ArithExpr, ArithOperator};
use between_expr::BetweenExpr;
use case_expr::CaseExpr;
use cast_expr::CastExpr;
use column_expr::ColumnExpr;
use comparison_expr::{ComparisonExpr, ComparisonOperator};
use conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use is_expr::IsExpr;
use literal_expr::LiteralExpr;
use negate_expr::NegateExpr;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::scalar::{OwnedScalarValue, ScalarValue};
use rayexec_error::{not_implemented, RayexecError, Result};
use scalar_function_expr::ScalarFunctionExpr;
use subquery_expr::SubqueryExpr;
use unnest_expr::UnnestExpr;
use window_expr::WindowExpr;

use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::functions::scalar::ScalarFunction;
use crate::logical::binder::bind_context::{BindContext, TableRef};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    Unnest(UnnestExpr),
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
            Self::Unnest(expr) => expr.datatype(bind_context)?,
        })
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
                for child in &mut conj.expressions {
                    func(child)?;
                }
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
            Self::Unnest(unnest) => func(&mut unnest.expr)?,
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
                for child in &conj.expressions {
                    func(child)?;
                }
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
            Self::Unnest(unnest) => func(&unnest.expr)?,
        }
        Ok(())
    }

    /// Replace this expression using a replacement function.
    pub fn replace_with<F>(&mut self, replace_fn: F)
    where
        F: FnOnce(Expression) -> Expression,
    {
        let expr = std::mem::replace(
            self,
            Expression::Literal(LiteralExpr {
                literal: ScalarValue::Null,
            }),
        );

        let out = replace_fn(expr);
        *self = out;
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

    pub fn contains_unnest(&self) -> bool {
        match self {
            Self::Unnest(_) => true,
            _ => {
                let mut has_unnest = false;
                self.for_each_child(&mut |expr| {
                    if has_unnest {
                        return Ok(());
                    }
                    has_unnest = has_unnest || expr.contains_unnest();
                    Ok(())
                })
                .expect("unnest check to no fail");
                has_unnest
            }
        }
    }

    // TODO: Probably remove.
    pub fn is_constant(&self) -> bool {
        match self {
            Self::Literal(_) => true,
            Self::Column(_) => false,
            _ => {
                let mut is_constant = true;
                self.for_each_child(&mut |expr| {
                    if !is_constant {
                        return Ok(());
                    }
                    is_constant = is_constant && expr.is_constant();
                    Ok(())
                })
                .expect("constant check to not fail");
                is_constant
            }
        }
    }

    pub fn is_const_foldable(&self) -> bool {
        match self {
            Self::Literal(v) => {
                match &v.literal {
                    ScalarValue::Null => {
                        // TODO: Not allowing null to be const foldable is
                        // currently a workaround for not have comprehensive
                        // support for evaluating null arrays without type
                        // information.
                        //
                        // Once we do, this case should be removed.
                        false
                    }
                    _ => true,
                }
            }
            Self::Column(_) => false,
            Self::Aggregate(_) => false,
            Self::Window(_) => false,
            Self::Subquery(_) => false, // Subquery shouldn't be in the plan anyways once this gets called.
            _ => {
                let mut is_foldable = true;
                self.for_each_child(&mut |expr| {
                    if !is_foldable {
                        return Ok(());
                    }
                    is_foldable = is_foldable && expr.is_const_foldable();
                    Ok(())
                })
                .expect("fold check to not fail");
                is_foldable
            }
        }
    }

    /// Get all column references in the expression.
    pub fn get_column_references(&self) -> Vec<ColumnExpr> {
        fn inner(expr: &Expression, cols: &mut Vec<ColumnExpr>) {
            match expr {
                Expression::Column(col) => cols.push(*col),
                other => other
                    .for_each_child(&mut |child| {
                        inner(child, cols);
                        Ok(())
                    })
                    .expect("not to fail"),
            }
        }

        let mut cols = Vec::new();
        inner(self, &mut cols);

        cols
    }

    pub fn get_table_references(&self) -> HashSet<TableRef> {
        fn inner(expr: &Expression, tables: &mut HashSet<TableRef>) {
            match expr {
                Expression::Column(col) => {
                    tables.insert(col.table_scope);
                }
                other => other
                    .for_each_child(&mut |child| {
                        inner(child, tables);
                        Ok(())
                    })
                    .expect("not to fail"),
            }
        }

        let mut tables = HashSet::new();
        inner(self, &mut tables);

        tables
    }

    pub const fn is_column_expr(&self) -> bool {
        matches!(self, Self::Column(_))
    }

    /// Try to get a top-level literal from this expression, erroring if it's
    /// not one.
    pub fn try_into_scalar(self) -> Result<OwnedScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit.literal),
            other => Err(RayexecError::new(format!("Not a literal: {other}"))),
        }
    }
}

pub fn add(left: Expression, right: Expression) -> Expression {
    Expression::Arith(ArithExpr {
        left: Box::new(left),
        right: Box::new(right),
        op: ArithOperator::Add,
    })
}

pub fn eq(left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpr {
        left: Box::new(left),
        right: Box::new(right),
        op: ComparisonOperator::Eq,
    })
}

pub fn lt(left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpr {
        left: Box::new(left),
        right: Box::new(right),
        op: ComparisonOperator::Lt,
    })
}

pub fn lt_eq(left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpr {
        left: Box::new(left),
        right: Box::new(right),
        op: ComparisonOperator::LtEq,
    })
}

pub fn gt(left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpr {
        left: Box::new(left),
        right: Box::new(right),
        op: ComparisonOperator::Gt,
    })
}

pub fn gt_eq(left: Expression, right: Expression) -> Expression {
    Expression::Comparison(ComparisonExpr {
        left: Box::new(left),
        right: Box::new(right),
        op: ComparisonOperator::GtEq,
    })
}

pub fn and(exprs: impl IntoIterator<Item = Expression>) -> Option<Expression> {
    let mut exprs: Vec<_> = exprs.into_iter().collect();
    if exprs.is_empty() {
        return None;
    }

    // ANDing one expression is the same as just the expression itself.
    if exprs.len() == 1 {
        return exprs.pop();
    }

    Some(Expression::Conjunction(ConjunctionExpr {
        op: ConjunctionOperator::And,
        expressions: exprs,
    }))
}

pub fn or(exprs: impl IntoIterator<Item = Expression>) -> Option<Expression> {
    let mut exprs: Vec<_> = exprs.into_iter().collect();
    if exprs.is_empty() {
        return None;
    }

    // ORing one expression is the same as just the expression itself.
    if exprs.len() == 1 {
        return exprs.pop();
    }

    Some(Expression::Conjunction(ConjunctionExpr {
        op: ConjunctionOperator::Or,
        expressions: exprs,
    }))
}

pub fn col_ref(table_ref: impl Into<TableRef>, column_idx: usize) -> Expression {
    Expression::Column(ColumnExpr {
        table_scope: table_ref.into(),
        column: column_idx,
    })
}

pub fn lit(scalar: impl Into<OwnedScalarValue>) -> Expression {
    Expression::Literal(LiteralExpr {
        literal: scalar.into(),
    })
}

pub fn cast(expr: Expression, to: DataType) -> Expression {
    Expression::Cast(CastExpr {
        to,
        expr: Box::new(expr),
    })
}

impl fmt::Display for Expression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_using_context(ContextDisplayMode::Raw, f)
    }
}

impl ContextDisplay for Expression {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        match self {
            Self::Aggregate(expr) => expr.fmt_using_context(mode, f),
            Self::Arith(expr) => expr.fmt_using_context(mode, f),
            Self::Between(expr) => expr.fmt_using_context(mode, f),
            Self::Case(expr) => expr.fmt_using_context(mode, f),
            Self::Cast(expr) => expr.fmt_using_context(mode, f),
            Self::Column(expr) => expr.fmt_using_context(mode, f),
            Self::Comparison(expr) => expr.fmt_using_context(mode, f),
            Self::Conjunction(expr) => expr.fmt_using_context(mode, f),
            Self::Is(expr) => expr.fmt_using_context(mode, f),
            Self::Literal(expr) => expr.fmt_using_context(mode, f),
            Self::Negate(expr) => expr.fmt_using_context(mode, f),
            Self::ScalarFunction(expr) => expr.fmt_using_context(mode, f),
            Self::Subquery(expr) => expr.fmt_using_context(mode, f),
            Self::Window(expr) => expr.fmt_using_context(mode, f),
            Self::Unnest(expr) => expr.fmt_using_context(mode, f),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_column_refs_simple() {
        let expr = and([
            col_ref(0, 0),
            col_ref(0, 1),
            or([col_ref(1, 8), col_ref(2, 4)]).unwrap(),
        ])
        .unwrap();

        let expected = vec![
            ColumnExpr {
                table_scope: 0.into(),
                column: 0,
            },
            ColumnExpr {
                table_scope: 0.into(),
                column: 1,
            },
            ColumnExpr {
                table_scope: 1.into(),
                column: 8,
            },
            ColumnExpr {
                table_scope: 2.into(),
                column: 4,
            },
        ];

        let got = expr.get_column_references();
        assert_eq!(expected, got);
    }
}
