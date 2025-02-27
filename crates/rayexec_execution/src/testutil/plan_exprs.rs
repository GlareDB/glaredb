//! Helpers for generating physical expressions from logical ones.

use crate::arrays::datatype::DataType;
use crate::expr::aggregate_expr::AggregateExpr;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::expr::physical::{PhysicalAggregateExpression, PhysicalScalarExpression};
use crate::expr::{self, Expression};
use crate::functions::function_set::AggregateFunctionSet;
use crate::logical::binder::table_list::{TableList, TableRef};

/// Wrapper for generating physical scalar expressions.
///
/// Table refs are extracted out of the expression, and sorted when planning.
#[track_caller]
pub fn plan_scalar(
    table_list: &TableList,
    expr: impl Into<Expression>,
) -> PhysicalScalarExpression {
    let planner = PhysicalExpressionPlanner::new(&table_list);
    let expr = expr.into();

    let mut table_refs: Vec<_> = expr.get_table_references().into_iter().collect();
    table_refs.sort();

    planner.plan_scalar(&table_refs, &expr).unwrap()
}

/// Plan many logical expressions that acts on a single set of inputs.
#[track_caller]
pub fn plan_scalars<'a>(
    table_list: &TableList,
    exprs: impl IntoIterator<Item = &'a Expression>,
) -> Vec<PhysicalScalarExpression> {
    exprs
        .into_iter()
        .map(|expr| plan_scalar(table_list, expr.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
    use crate::functions::aggregate::builtin::sum;

    #[test]
    fn plan_literal() {
        let empty = TableList::empty();
        let phys = plan_scalar(&empty, expr::lit("cat"));

        match phys {
            PhysicalScalarExpression::Literal(lit) => {
                assert_eq!("cat", lit.literal.try_as_str().unwrap())
            }
            other => panic!("unexpected physical expression: {other:?}"),
        }
    }

    #[test]
    fn plan_column_ref() {
        let mut list = TableList::empty();
        let t0 = list
            .push_table(None, [DataType::Int32, DataType::Utf8], ["c1", "c2"])
            .unwrap();
        let phys = plan_scalar(&list, list.column_as_expr((t0, 1)).unwrap());

        match phys {
            PhysicalScalarExpression::Column(col) => {
                assert_eq!(1, col.idx);
                assert_eq!(DataType::Utf8, col.datatype)
            }
            other => panic!("unexpected physical expression: {other:?}"),
        }
    }
}
