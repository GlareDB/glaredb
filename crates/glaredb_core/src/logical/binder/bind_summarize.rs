use glaredb_error::Result;
use glaredb_parser::ast;

use super::bind_context::{BindContext, BindScopeRef};
use super::bind_query::BoundQuery;
use super::table_list::TableType;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::ScalarValue;
use crate::expr::aggregate_expr::AggregateExpr;
use crate::expr::scalar_function_expr::ScalarFunctionExpr;
use crate::expr::{self, Expression};
use crate::functions::aggregate::builtin::avg::FUNCTION_SET_AVG;
use crate::functions::aggregate::builtin::count::FUNCTION_SET_COUNT;
use crate::functions::aggregate::builtin::minmax::{FUNCTION_SET_MAX, FUNCTION_SET_MIN};
use crate::functions::scalar::builtin::list::FUNCTION_SET_LIST_VALUE;
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::binder::bind_query::bind_from::FromBinder;
use crate::logical::binder::bind_query::bind_select::BoundSelect;
use crate::logical::binder::bind_query::select_list::{BoundDistinctModifier, BoundSelectList};
use crate::logical::resolver::ResolvedMeta;
use crate::logical::resolver::resolve_context::ResolveContext;

#[derive(Debug)]
pub struct SummarizeBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> SummarizeBinder<'a> {
    pub fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        SummarizeBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_summarize(
        &self,
        bind_context: &mut BindContext,
        summarize: ast::Summarize<ResolvedMeta>,
    ) -> Result<BoundQuery> {
        // Each column in the input to the summarize will have a set of
        // aggregates applied to it. Each aggregate "set" will be placed in a
        // list which will then get unnested into a proper table.
        //
        // Unnest is used over a union to avoid multiple scans.

        // TODO: Change this when results unnested.
        let table_ref = bind_context.push_table(
            self.current,
            None,
            [
                DataType::list(DataType::utf8()),
                DataType::list(DataType::utf8()),
                DataType::list(DataType::int64()),
                DataType::list(DataType::utf8()),
                DataType::list(DataType::utf8()),
                DataType::list(DataType::utf8()),
            ],
            ["column_name", "datatype", "count", "avg", "min", "max"],
        )?;

        let query_scope = bind_context.new_orphan_scope();

        let from = match summarize {
            ast::Summarize::Query(query) => {
                let _ = QueryBinder::new(query_scope, self.resolve_context)
                    .bind(bind_context, query)?;
                unimplemented!()
            }
            ast::Summarize::FromNode(from) => {
                FromBinder::new(query_scope, self.resolve_context).bind(bind_context, Some(from))?
            }
        };

        let aggregates_table = bind_context.new_ephemeral_table()?;
        // All aggregates.
        let mut aggregates: Vec<Expression> = Vec::new();

        // Helper to bind and append an aggregate function.
        //
        // The bound agg function will be appended to `aggs`, and a projection
        // pointing to that aggregate appended to `projections`.
        //
        // `cast_string` indicates if we should cast the output of the aggregate
        // to a string. Needed for aggs like min/max which will have different
        // types that we'd need to handle.
        let mut append_summarize_aggregate = |function,
                                              column_expr: Expression,
                                              projections: &mut Vec<_>,
                                              cast_string: bool,
                                              require_numeric: bool|
         -> Result<()> {
            if require_numeric && !column_expr.datatype()?.is_numeric() {
                // Not a numeric column, can't run statistics aggregate on it.
                // Just push a constant null value.
                projections.push(expr::lit(ScalarValue::Null).into());
                return Ok(());
            }

            let agg_fn = expr::bind_aggregate_function(function, vec![column_expr])?;
            let agg_col_idx = aggregates.len();
            let mut agg_proj: Expression = expr::column(
                (aggregates_table, agg_col_idx),
                agg_fn.state.return_type.clone(),
            )
            .into();
            let agg_expr = Expression::Aggregate(AggregateExpr {
                agg: agg_fn,
                filter: None,
                distinct: false,
            });

            if cast_string {
                agg_proj = expr::cast(agg_proj, DataType::utf8())?.into();
            }

            aggregates.push(agg_expr);
            projections.push(agg_proj);

            Ok(())
        };

        let mut column_names: Vec<Expression> = Vec::new();
        let mut datatypes: Vec<Expression> = Vec::new();

        let mut counts_projections: Vec<Expression> = Vec::new();
        let mut avg_projections: Vec<Expression> = Vec::new();
        let mut min_projections: Vec<Expression> = Vec::new();
        let mut max_projections: Vec<Expression> = Vec::new();

        let tables = bind_context
            .iter_tables_in_scope(query_scope)?
            .filter(|t| t.table_type == TableType::Data);

        for table in tables {
            let columns = table.column_names.iter().zip(&table.column_types);

            for (col_idx, (column_name, datatype)) in columns.enumerate() {
                // "Static" values
                column_names.push(expr::lit(column_name.as_raw_str().to_string()).into());
                datatypes.push(expr::lit(format!("{datatype}")).into());

                // Aggregate values - since we're manually building the bound
                // select list, we have to track the aggregate expressions and
                // projections separately.
                let col = expr::column((table.reference, col_idx), datatype.clone());

                append_summarize_aggregate(
                    &FUNCTION_SET_COUNT,
                    col.clone(),
                    &mut counts_projections,
                    false,
                    false,
                )?;

                append_summarize_aggregate(
                    &FUNCTION_SET_AVG,
                    col.clone(),
                    &mut avg_projections,
                    true,
                    true,
                )?;

                append_summarize_aggregate(
                    &FUNCTION_SET_MIN,
                    col.clone(),
                    &mut min_projections,
                    false,
                    true,
                )?;

                append_summarize_aggregate(
                    &FUNCTION_SET_MAX,
                    col.clone(),
                    &mut max_projections,
                    false,
                    true,
                )?;
            }
        }

        // Create lists from the projections.
        let create_list_expr = |projections| -> Result<Expression> {
            let list_fn = expr::bind_scalar_function(&FUNCTION_SET_LIST_VALUE, projections)?;
            Ok(Expression::ScalarFunction(ScalarFunctionExpr {
                function: list_fn,
            }))
        };

        let names_list = create_list_expr(column_names)?;
        let datatypes_list = create_list_expr(datatypes)?;
        let count_list = create_list_expr(counts_projections)?;
        let avg_list = create_list_expr(avg_projections)?;
        let min_list = create_list_expr(min_projections)?;
        let max_list = create_list_expr(max_projections)?;

        let select_list = BoundSelectList {
            output: None,
            projections_table: table_ref,
            projections: vec![
                names_list,
                datatypes_list,
                count_list,
                avg_list,
                min_list,
                max_list,
            ],
            aggregates_table,
            aggregates,
            windows_table: bind_context.new_ephemeral_table()?,
            windows: Vec::new(),
            grouping_functions_table: bind_context.new_ephemeral_table()?,
            grouping_functions: Vec::new(),
            distinct_modifier: BoundDistinctModifier::All,
        };

        let select = BoundSelect {
            select_list,
            from,
            filter: None,
            having: None,
            group_by: None,
            order_by: None,
            limit: None,
            groupings: Vec::new(),
        };

        Ok(BoundQuery::Select(select))
    }
}
