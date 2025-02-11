//! Helpers for generating physical expressions from logical ones.

use crate::arrays::datatype::DataType;
use crate::expr::physical::planner::PhysicalExpressionPlanner;
use crate::expr::physical::{PhysicalAggregateExpression, PhysicalScalarExpression};
use crate::expr::{self, Expression};
use crate::functions::aggregate::AggregateFunction;
use crate::logical::binder::table_list::{TableList, TableRef};

/// Plans a logical expression.
///
/// `inputs` represents the input tables that the expression can reference.
/// Column references contained in the expression will index into `inputs`
/// directly (e.g. ColumnExpr{table_ref: 0, column: 2} will look at
/// inputs[0][2]).
#[track_caller]
pub fn plan_scalar(expr: &Expression, inputs: &[&[DataType]]) -> PhysicalScalarExpression {
    let (table_list, table_refs) = create_table_list(inputs);
    let planner = PhysicalExpressionPlanner::new(&table_list);
    planner.plan_scalar(&table_refs, expr).unwrap()
}

/// Plan many logical expressions that acts on a single set of inputs.
#[track_caller]
pub fn plan_scalars<'a>(
    exprs: impl IntoIterator<Item = &'a Expression>,
    inputs: &[&[DataType]],
) -> Vec<PhysicalScalarExpression> {
    let (table_list, table_refs) = create_table_list(inputs);
    let planner = PhysicalExpressionPlanner::new(&table_list);
    planner.plan_scalars(&table_refs, exprs).unwrap()
}

/// Plan an aggregate function.
///
/// This assumes the input to the aggregate is a column whose types are provided
/// by `inputs.
///
/// This cannot be used to plan multiple aggregates for a single operator.
pub fn plan_aggregate(
    agg: &dyn AggregateFunction,
    inputs: impl IntoIterator<Item = DataType>,
) -> PhysicalAggregateExpression {
    let inputs: Vec<_> = inputs.into_iter().collect();
    let (table_list, refs) = create_table_list(&[&inputs]);
    assert_eq!(1, refs.len());

    let exprs: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(idx, _)| expr::col_ref(refs[0], idx))
        .collect();

    let planned = agg.plan(&table_list, exprs).unwrap();
    let columns: Vec<_> = plan_scalars(&planned.inputs, &[&inputs])
        .into_iter()
        .map(|expr| match expr {
            PhysicalScalarExpression::Column(col) => col,
            other => panic!("Not a column expr: {other:?}"),
        })
        .collect();

    PhysicalAggregateExpression {
        function: planned,
        columns,
        is_distinct: false,
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TestAggregate {
    pub function: &'static dyn AggregateFunction,
    pub columns: &'static [usize],
}

/// Plans multiple aggregates.
///
/// Aggregates a provides in (function, column) pairs. The column indices are
/// used to index into the input datatypes.
pub fn plan_aggregates<'a>(
    aggs: impl IntoIterator<Item = TestAggregate>,
    inputs: impl IntoIterator<Item = DataType>,
) -> Vec<PhysicalAggregateExpression> {
    let inputs: Vec<_> = inputs.into_iter().collect();
    let (table_list, refs) = create_table_list(&[&inputs]);
    assert_eq!(1, refs.len());

    let mut phys_aggs = Vec::new();

    for agg in aggs {
        let exprs: Vec<_> = agg
            .columns
            .iter()
            .map(|&idx| expr::col_ref(refs[0], idx))
            .collect();

        let planned = agg.function.plan(&table_list, exprs).unwrap();
        let columns: Vec<_> = plan_scalars(&planned.inputs, &[&inputs])
            .into_iter()
            .map(|expr| match expr {
                PhysicalScalarExpression::Column(col) => col,
                other => panic!("Not a column expr: {other:?}"),
            })
            .collect();

        phys_aggs.push(PhysicalAggregateExpression {
            function: planned,
            columns,
            is_distinct: false,
        })
    }

    phys_aggs
}

#[track_caller]
fn create_table_list(inputs: &[&[DataType]]) -> (TableList, Vec<TableRef>) {
    let mut table_list = TableList::empty();
    let mut table_refs = Vec::with_capacity(inputs.len());

    for (idx, &input) in inputs.iter().enumerate() {
        let types = input.to_vec();
        let names = (0..types.len()).map(|idx| format!("col{idx}")).collect();

        let reference = table_list.push_table(None, types, names).unwrap();
        // This will need updating if we change how we generate table
        // references.
        assert_eq!(idx, reference.table_idx);
        table_refs.push(reference);
    }

    (table_list, table_refs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expr;
    use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
    use crate::functions::aggregate::builtin::sum;

    #[test]
    fn plan_literal() {
        let phys = plan_scalar(&expr::lit("cat"), &[]);

        match phys {
            PhysicalScalarExpression::Literal(lit) => {
                assert_eq!("cat", lit.literal.try_as_str().unwrap())
            }
            other => panic!("unexpected physical expression: {other:?}"),
        }
    }

    #[test]
    fn plan_column_ref() {
        let phys = plan_scalar(&expr::col_ref(0, 1), &[&[DataType::Int32, DataType::Utf8]]);

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
        let _agg = plan_aggregate(&sum::Sum, [DataType::Int64]);
    }
}
