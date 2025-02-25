pub mod aggregate_expr;
pub mod arith_expr;
pub mod between_expr;
pub mod case_expr;
pub mod cast_expr;
pub mod column_expr;
pub mod comparison_expr;
pub mod conjunction_expr;
pub mod grouping_set_expr;
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
use fmtutil::IntoDisplayableSlice;
use grouping_set_expr::GroupingSetExpr;
use is_expr::IsExpr;
use literal_expr::LiteralExpr;
use negate_expr::{NegateExpr, NegateOperator};
use rayexec_error::{RayexecError, Result};
use scalar_function_expr::ScalarFunctionExpr;
use subquery_expr::SubqueryExpr;
use unnest_expr::UnnestExpr;
use window_expr::WindowExpr;

use crate::arrays::datatype::DataType;
use crate::arrays::scalar::{BorrowedScalarValue, ScalarValue};
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode};
use crate::functions::function_set::{FunctionInfo, FunctionSet, ScalarFunctionSet};
use crate::functions::scalar::{FunctionVolatility, PlannedScalarFunction};
use crate::functions::CastType;
use crate::logical::binder::table_list::{TableList, TableRef};

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
    GroupingSet(GroupingSetExpr),
}

impl Expression {
    /// Get the return type of the expression.
    ///
    /// The provided table list is used when resolving the return type for a
    /// column expression.
    // TODO: Ideally we can remove the table list arg, but that would require
    // some planning rework (specifically around referencing user aliases and
    // column ordinals in GROUP BY and ORDER BY).
    pub fn datatype(&self, table_list: &TableList) -> Result<DataType> {
        Ok(match self {
            Self::Aggregate(expr) => expr.agg.return_type.clone(),
            Self::Arith(expr) => expr.return_type.clone(),
            Self::Between(_) => DataType::Boolean,
            Self::Case(expr) => expr.datatype(table_list)?,
            Self::Cast(expr) => expr.to.clone(),
            Self::Column(expr) => expr.datatype(table_list)?,
            Self::Comparison(_) => DataType::Boolean,
            Self::Conjunction(_) => DataType::Boolean,
            Self::Is(_) => DataType::Boolean,
            Self::Literal(expr) => expr.literal.datatype(),
            Self::Negate(expr) => expr.datatype(table_list)?,
            Self::ScalarFunction(expr) => expr.function.state.return_type.clone(),
            Self::Subquery(expr) => expr.return_type.clone(),
            Self::Window(window) => window.agg.return_type.clone(),
            Self::Unnest(expr) => expr.datatype(table_list)?,
            Self::GroupingSet(expr) => expr.datatype(),
        })
    }

    pub fn for_each_child_mut<F>(&mut self, func: &mut F) -> Result<()>
    where
        F: FnMut(&mut Expression) -> Result<()>,
    {
        match self {
            Self::Aggregate(agg) => {
                for expr in &mut agg.agg.inputs {
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
                for input in &mut scalar.function.state.inputs {
                    func(input)?;
                }
            }
            Self::Subquery(_) => (),
            Self::Window(window) => {
                for input in &mut window.agg.inputs {
                    func(input)?;
                }
                for partition in &mut window.partition_by {
                    func(partition)?;
                }
                for order_by in &mut window.order_by {
                    func(&mut order_by.expr)?;
                }
            }
            Self::Unnest(unnest) => func(&mut unnest.expr)?,
            Self::GroupingSet(grouping) => {
                for input in &mut grouping.inputs {
                    func(input)?;
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
                for expr in &agg.agg.inputs {
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
                for input in &scalar.function.state.inputs {
                    func(input)?;
                }
            }
            Self::Subquery(_) => (),
            Self::Window(window) => {
                for input in &window.agg.inputs {
                    func(input)?;
                }
                for partition in &window.partition_by {
                    func(partition)?;
                }
                for order_by in &window.order_by {
                    func(&order_by.expr)?;
                }
            }
            Self::Unnest(unnest) => func(&unnest.expr)?,
            Self::GroupingSet(grouping) => {
                for input in &grouping.inputs {
                    func(input)?;
                }
            }
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
                literal: BorrowedScalarValue::Null,
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
                .expect("subquery check to not fail");
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
                .expect("unnest check to not fail");
                has_unnest
            }
        }
    }

    pub fn contains_window(&self) -> bool {
        match self {
            Self::Window(_) => true,
            _ => {
                let mut has_window = false;
                self.for_each_child(&mut |expr| {
                    if has_window {
                        return Ok(());
                    }
                    has_window = has_window || expr.contains_window();
                    Ok(())
                })
                .expect("window check to not fail");
                has_window
            }
        }
    }

    /// Checks if this expression can be folded into a constant.
    pub fn is_const_foldable(&self) -> bool {
        // Encountering any column means we can't fold.
        self.is_const_foldable_with_column_check(&|_col| false)
    }

    /// Checks if this expression can be folded into a constant assuming that
    /// the given column expression is fixed.
    ///
    /// This will return true if the only columns encountered equal the fixed
    /// column, and if the rest of the epxression is const foldable.
    pub fn is_const_foldable_with_fixed_column(&self, fixed: &ColumnExpr) -> bool {
        self.is_const_foldable_with_column_check(&|col| col == fixed)
    }

    /// Helper function when checking if an expression is const foldable.
    ///
    /// `check_col` indicates the behavior when encountering a column
    /// expression.
    fn is_const_foldable_with_column_check<F>(&self, check_col: &F) -> bool
    where
        F: Fn(&ColumnExpr) -> bool,
    {
        match self {
            Self::Literal(v) => {
                match &v.literal {
                    BorrowedScalarValue::Null => {
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
            Self::Column(col) => check_col(col),
            Self::Aggregate(_) => false,
            Self::Window(_) => false,
            Self::Subquery(_) => false, // Subquery shouldn't be in the plan anyways once this gets called.
            Self::ScalarFunction(f)
                if f.function.raw.volatility() == FunctionVolatility::Volatile =>
            {
                false
            }
            _ => {
                let mut is_foldable = true;
                self.for_each_child(&mut |expr| {
                    if !is_foldable {
                        return Ok(());
                    }
                    is_foldable =
                        is_foldable && expr.is_const_foldable_with_column_check(check_col);
                    Ok(())
                })
                .expect("fold check to not fail");
                is_foldable
            }
        }
    }

    /// Replace all instances of `from` with `to`.
    pub fn replace_column(mut self, from: ColumnExpr, to: ColumnExpr) -> Self {
        fn inner(expr: &mut Expression, from: ColumnExpr, to: ColumnExpr) {
            match expr {
                Expression::Column(col) => {
                    if col == &from {
                        *col = to
                    }
                }
                other => other
                    .for_each_child_mut(&mut |child| {
                        inner(child, from, to);
                        Ok(())
                    })
                    .expect("replace to not fail"),
            }
        }

        inner(&mut self, from, to);
        self
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
    pub fn try_into_scalar(self) -> Result<ScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit.literal),
            other => Err(RayexecError::new(format!("Not a literal: {other}"))),
        }
    }
}

macro_rules! impl_from_expr {
    ($variant:ident, $expr:ident) => {
        impl From<$expr> for Expression {
            fn from(expr: $expr) -> Self {
                Expression::$variant(expr)
            }
        }
    };
}

impl_from_expr!(Aggregate, AggregateExpr);
impl_from_expr!(Arith, ArithExpr);
impl_from_expr!(Between, BetweenExpr);
impl_from_expr!(Case, CaseExpr);
impl_from_expr!(Cast, CastExpr);
impl_from_expr!(Column, ColumnExpr);
impl_from_expr!(Comparison, ComparisonExpr);
impl_from_expr!(Conjunction, ConjunctionExpr);
impl_from_expr!(Is, IsExpr);
impl_from_expr!(Literal, LiteralExpr);
impl_from_expr!(Negate, NegateExpr);
impl_from_expr!(ScalarFunction, ScalarFunctionExpr);
impl_from_expr!(Unnest, UnnestExpr);
impl_from_expr!(Window, WindowExpr);

pub fn compare(
    table_list: &TableList,
    op: ComparisonOperator,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    let (_, [left, right]) = bind_function_signature_fixed(
        table_list,
        op.as_scalar_function_set(),
        [left.into(), right.into()],
    )?;

    Ok(ComparisonExpr {
        op,
        left: Box::new(left),
        right: Box::new(right),
    })
}

pub fn arith(
    table_list: &TableList,
    op: ArithOperator,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ArithExpr> {
    let (raw, inputs) = bind_function_signature(
        table_list,
        op.as_scalar_function_set(),
        vec![left.into(), right.into()],
    )?;

    // TODO: Avoid
    let state = raw.call_bind(table_list, inputs)?;
    let return_type = state.return_type;
    let [left, right] = state.inputs.try_into().unwrap();

    Ok(ArithExpr {
        op,
        left: Box::new(left),
        right: Box::new(right),
        return_type,
    })
}

pub fn conjunction(
    table_list: &TableList,
    op: ConjunctionOperator,
    inputs: impl IntoIterator<Item = Expression>,
) -> Result<ConjunctionExpr> {
    let (_, inputs) = bind_function_signature(
        table_list,
        op.as_scalar_function_set(),
        inputs.into_iter().collect(),
    )?;

    Ok(ConjunctionExpr {
        op,
        expressions: inputs,
    })
}

pub fn negate(
    table_list: &TableList,
    op: NegateOperator,
    input: impl Into<Expression>,
) -> Result<NegateExpr> {
    let (_, [input]) =
        bind_function_signature_fixed(table_list, op.as_scalar_function_set(), [input.into()])?;

    Ok(NegateExpr {
        op,
        expr: Box::new(input),
    })
}

pub fn add(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ArithExpr> {
    arith(table_list, ArithOperator::Add, left, right)
}

pub fn sub(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ArithExpr> {
    arith(table_list, ArithOperator::Sub, left, right)
}

pub fn mul(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ArithExpr> {
    arith(table_list, ArithOperator::Mul, left, right)
}

pub fn div(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ArithExpr> {
    arith(table_list, ArithOperator::Div, left, right)
}

pub fn eq(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    compare(table_list, ComparisonOperator::Eq, left, right)
}

pub fn not_eq(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    compare(table_list, ComparisonOperator::NotEq, left, right)
}

pub fn lt(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    compare(table_list, ComparisonOperator::Lt, left, right)
}

pub fn lt_eq(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    compare(table_list, ComparisonOperator::LtEq, left, right)
}

pub fn gt(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    compare(table_list, ComparisonOperator::Gt, left, right)
}

pub fn gt_eq(
    table_list: &TableList,
    left: impl Into<Expression>,
    right: impl Into<Expression>,
) -> Result<ComparisonExpr> {
    compare(table_list, ComparisonOperator::GtEq, left, right)
}

pub fn and(
    table_list: &TableList,
    exprs: impl IntoIterator<Item = Expression>,
) -> Result<ConjunctionExpr> {
    conjunction(table_list, ConjunctionOperator::And, exprs)
}

pub fn or(
    table_list: &TableList,
    exprs: impl IntoIterator<Item = Expression>,
) -> Result<ConjunctionExpr> {
    conjunction(table_list, ConjunctionOperator::Or, exprs)
}

pub fn col_ref(table_ref: impl Into<TableRef>, column_idx: usize) -> Expression {
    Expression::Column(ColumnExpr {
        table_scope: table_ref.into(),
        column: column_idx,
    })
}

pub fn lit(scalar: impl Into<ScalarValue>) -> LiteralExpr {
    LiteralExpr {
        literal: scalar.into(),
    }
}

pub fn cast(expr: Expression, to: DataType) -> CastExpr {
    CastExpr {
        to,
        expr: Box::new(expr),
    }
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
            Self::GroupingSet(expr) => expr.fmt_using_context(mode, f),
        }
    }
}

pub trait AsScalarFunctionSet {
    /// Returns the the function set for an operator.
    ///
    /// Called during planning to get the function implementation for arith,
    /// comparision, etc operators to apply appropriate casts to the input
    /// expressions.
    fn as_scalar_function_set(&self) -> &ScalarFunctionSet;
}

/// Binds a scalar function with the given inputs.
///
/// This will cast the inputs as needed.
pub fn bind_scalar_function(
    table_list: &TableList,
    function: &ScalarFunctionSet,
    inputs: Vec<Expression>,
) -> Result<PlannedScalarFunction> {
    let (func, inputs) = bind_function_signature(table_list, function, inputs)?;
    let bind_state = func.call_bind(table_list, inputs)?;

    Ok(PlannedScalarFunction {
        name: function.name,
        raw: func,
        state: bind_state,
    })
}

/// Convenience function for working with fixed sized inputs.
pub(crate) fn bind_function_signature_fixed<F, const N: usize>(
    table_list: &TableList,
    function: &FunctionSet<F>,
    inputs: [Expression; N],
) -> Result<(F, [Expression; N])>
where
    F: FunctionInfo,
{
    let (f, inputs) = bind_function_signature(table_list, function, inputs.to_vec())?;
    let inputs = inputs
        .try_into()
        .map_err(|_| RayexecError::new("failed to convert to array"))?;
    Ok((f, inputs))
}

/// Find the the best function to use from the function based on signature,
/// returning the raw function and possibly updated expressions.
///
/// If the there isn't an exact match, but a candidate exists, casts will be
/// applied to match the closest signature.
pub(crate) fn bind_function_signature<F>(
    table_list: &TableList,
    function: &FunctionSet<F>,
    mut inputs: Vec<Expression>,
) -> Result<(F, Vec<Expression>)>
where
    F: FunctionInfo,
{
    let datatypes = inputs
        .iter()
        .map(|expr| expr.datatype(table_list))
        .collect::<Result<Vec<_>>>()?;

    let func = match function.find_exact(&datatypes) {
        Some(func) => func,
        None => {
            // No exact, try to see if there's candidate.
            let mut candidates = function.candidates(&datatypes);

            if candidates.is_empty() {
                // TODO: Better error.
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    function.name,
                    datatypes.display_with_brackets(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            //
            // We should do some lightweight const folding and prefer candidates
            // that cast the consts over ones that need array inputs to be
            // casted.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => Expression::Cast(CastExpr {
                            to: DataType::try_default_datatype(to)?, // TODO: Still want this?
                            expr: Box::new(input),
                        }),
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            function
                .get(candidate.signature_idx)
                .expect("candidate to return value index")
        }
    };

    // TODO: Further refine the types (e.g. aligning precision/scale for
    // decimals).

    Ok((*func, inputs))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_column_refs_simple() {
        let mut list = TableList::empty();
        let t0 = list
            .push_table(
                None,
                vec![DataType::Int64, DataType::Int64],
                vec!["c1".to_string(), "c2".to_string()],
            )
            .unwrap();
        let t1 = list
            .push_table(
                None,
                vec![
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                    DataType::Int64,
                ],
                vec![
                    "c1".to_string(),
                    "c2".to_string(),
                    "c3".to_string(),
                    "c4".to_string(),
                ],
            )
            .unwrap();

        let expr: Expression = and(
            &list,
            [
                col_ref(t0, 0).into(),
                col_ref(t0, 1).into(),
                or(&list, [col_ref(t1, 4), col_ref(t1, 2)]).unwrap().into(),
            ],
        )
        .unwrap()
        .into();

        let expected = vec![
            ColumnExpr {
                table_scope: t0,
                column: 0,
            },
            ColumnExpr {
                table_scope: t0,
                column: 1,
            },
            ColumnExpr {
                table_scope: t1,
                column: 4,
            },
            ColumnExpr {
                table_scope: t1,
                column: 2,
            },
        ];

        let got = expr.get_column_references();
        assert_eq!(expected, got);
    }

    #[test]
    fn is_const_foldable() {
        let empty = TableList::empty();
        let expr: Expression = and(
            &empty,
            [
                gt_eq(&empty, add(&empty, lit(4), lit(8)).unwrap(), lit(12))
                    .unwrap()
                    .into(), // ((4 + 8) >= 12)
                lit(false).into(),
            ],
        )
        .unwrap()
        .into();

        let is_foldable = expr.is_const_foldable();
        assert!(is_foldable);

        let mut list = TableList::empty();
        let t0 = list
            .push_table(None, vec![DataType::Int32], vec!["c1".to_string()])
            .unwrap();

        let expr: Expression = and(
            &list,
            [
                gt_eq(&list, add(&list, lit(4), lit(8)).unwrap(), col_ref(t0, 0))
                    .unwrap()
                    .into(), // ((4 + 8) >= #column)
                lit(false).into(),
            ],
        )
        .unwrap()
        .into();

        let is_foldable = expr.is_const_foldable();
        assert!(!is_foldable);
    }

    #[test]
    fn is_const_foldable_fixed() {
        let mut list = TableList::empty();
        let t0 = list
            .push_table(None, vec![DataType::Int32], vec!["c1".to_string()])
            .unwrap();

        let expr: Expression = and(
            &list,
            [
                gt_eq(&list, add(&list, lit(4), lit(8)).unwrap(), col_ref(t0, 0))
                    .unwrap()
                    .into(), // ((4 + 8) >= #column)
                lit(false).into(),
            ],
        )
        .unwrap()
        .into();

        let is_foldable = expr.is_const_foldable_with_fixed_column(&ColumnExpr::new(t0, 0));
        assert!(is_foldable);

        let is_foldable = expr.is_const_foldable_with_fixed_column(&ColumnExpr::new(12, 1));
        assert!(!is_foldable);
    }
}
