//! Helpers for generating physical expressions from logical ones.

use crate::arrays::datatype::DataType;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::expr::physical::{PhysicalAggregateExpression, PhysicalScalarExpression};
use crate::expr::{self, Expression};
use crate::functions::aggregate::AggregateFunction;
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
    exprs: impl IntoIterator<Item = &'a Expression>,
    inputs: &[&[DataType]],
) -> Vec<PhysicalScalarExpression> {
    unimplemented!()
    // let (table_list, table_refs) = create_table_list(inputs);
    // let planner = PhysicalExpressionPlanner::new(&table_list);
    // planner.plan_scalars(&table_refs, exprs).unwrap()
}

#[derive(Debug, Clone, Copy)]
pub struct TestAggregate {
    pub function: &'static dyn AggregateFunction,
    pub columns: &'static [usize],
}

/// Plan an aggregate function.
///
/// `inputs` is expected to be the complete input, while `agg` specifies which
/// columns from the input to use for its inputs.
pub fn plan_aggregate(
    agg: TestAggregate,
    inputs: impl IntoIterator<Item = DataType>,
) -> PhysicalAggregateExpression {
    unimplemented!()
    // let inputs: Vec<_> = inputs.into_iter().collect();
    // let (table_list, refs) = create_table_list(&[&inputs]);
    // assert_eq!(1, refs.len());

    // let exprs: Vec<_> = agg
    //     .columns
    //     .iter()
    //     .map(|&idx| expr::col_ref(refs[0], idx))
    //     .collect();

    // let planned = agg.function.plan(&table_list, exprs).unwrap();
    // let columns: Vec<_> = plan_scalars(&planned.inputs, &[&inputs])
    //     .into_iter()
    //     .map(|expr| match expr {
    //         PhysicalScalarExpression::Column(col) => col,
    //         other => panic!("Not a column expr: {other:?}"),
    //     })
    //     .collect();

    // PhysicalAggregateExpression {
    //     function: planned,
    //     columns,
    //     is_distinct: false,
    // }
}

/// Plans multiple aggregates.
///
/// Aggregates a provides in (function, column) pairs. The column indices are
/// used to index into the input datatypes.
pub fn plan_aggregates<'a>(
    aggs: impl IntoIterator<Item = TestAggregate>,
    inputs: impl IntoIterator<Item = DataType>,
) -> Vec<PhysicalAggregateExpression> {
    unimplemented!()
    // let inputs: Vec<_> = inputs.into_iter().collect();
    // let (table_list, refs) = create_table_list(&[&inputs]);
    // assert_eq!(1, refs.len());

    // let mut phys_aggs = Vec::new();

    // for agg in aggs {
    //     let exprs: Vec<_> = agg
    //         .columns
    //         .iter()
    //         .map(|&idx| expr::col_ref(refs[0], idx))
    //         .collect();

    //     let planned = agg.function.plan(&table_list, exprs).unwrap();
    //     let columns: Vec<_> = plan_scalars(&planned.inputs, &[&inputs])
    //         .into_iter()
    //         .map(|expr| match expr {
    //             PhysicalScalarExpression::Column(col) => col,
    //             other => panic!("Not a column expr: {other:?}"),
    //         })
    //         .collect();

    //     phys_aggs.push(PhysicalAggregateExpression {
    //         function: planned,
    //         columns,
    //         is_distinct: false,
    //     })
    // }

    // phys_aggs
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
        let phys = plan_scalar(&list, expr::col_ref(t0, 1));

        match phys {
            PhysicalScalarExpression::Column(col) => {
                assert_eq!(1, col.idx);
                assert_eq!(DataType::Utf8, col.datatype)
            }
            other => panic!("unexpected physical expression: {other:?}"),
        }
    }

    #[test]
    fn plan_aggregate_sanity() {
        let _agg = plan_aggregate(
            TestAggregate {
                function: &sum::Sum,
                columns: &[0],
            },
            [DataType::Int64],
        );
    }
}
