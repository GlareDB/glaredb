use datafusion::execution::context::{ExecutionProps, SessionState};

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::{internal_err, not_impl_err, plan_err, DFSchema, ScalarValue};
use datafusion::logical_expr::expr::{
    self, AggregateFunction, AggregateUDF, Alias, Between, Cast, GetFieldAccess, GetIndexedField,
    GroupingSet, InList, Like, ScalarFunction, ScalarUDF, TryCast, WindowFunction,
};
use datafusion::logical_expr::{WindowFrame, WindowFrameBound};
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_plan::aggregates::PhysicalGroupBy;
use datafusion::physical_plan::expressions::{
    self, binary, like, Column, GetFieldAccessExpr, GetIndexedFieldExpr, PhysicalSortExpr,
};

use datafusion::physical_plan::{aggregates, functions, udaf, udf, windows};
use datafusion::physical_plan::{AggregateExpr, PhysicalExpr, WindowExpr};

use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::binary_expr;
use datafusion::variable::VarType;
use itertools::Itertools;
use std::fmt::Write;
use std::sync::Arc;

/// Create a physical expression from a logical expression ([Expr]).
///
/// # Arguments
///
/// * `e` - The logical expression
/// * `input_dfschema` - The DataFusion schema for the input, used to resolve `Column` references
///                      to qualified or unqualified fields by name.
/// * `input_schema` - The Arrow schema for the input, used for determining expression data types
///                    when performing type coercion.
pub fn create_physical_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn PhysicalExpr>> {
    println!("create_physical_expr: {:?}", e);
    if input_schema.fields.len() != input_dfschema.fields().len() {
        return internal_err!(
            "create_physical_expr expected same number of fields, got \
                     Arrow schema with {}  and DataFusion schema with {}",
            input_schema.fields.len(),
            input_dfschema.fields().len()
        );
    }
    match e {
        Expr::Placeholder(p) => {
            todo!("Placeholder")
            // let index = p.index;
            // let data_type = input_schema.field(index).data_type().clone();
            // Ok(Arc::new(expressions::placeholder(data_type, index)))
        }
        Expr::Alias(Alias { expr, .. }) => Ok(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Column(c) => {
            let idx = input_dfschema.index_of_column(c)?;
            Ok(Arc::new(Column::new(&c.name, idx)))
        }
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        Expr::ScalarVariable(_, variable_names) => {
            match execution_props.get_var_provider(VarType::System) {
                Some(provider) => {
                    let scalar_value = provider.get_value(variable_names.clone())?;
                    Ok(Arc::new(Literal::new(scalar_value)))
                }
                _ => plan_err!("No system variable provider found"),
            }
        }
        Expr::IsTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(&binary_op, input_dfschema, input_schema, execution_props)
        }
        Expr::IsNotTrue(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(true))),
            );
            create_physical_expr(&binary_op, input_dfschema, input_schema, execution_props)
        }
        Expr::IsFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(&binary_op, input_dfschema, input_schema, execution_props)
        }
        Expr::IsNotFalse(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(Some(false))),
            );
            create_physical_expr(&binary_op, input_dfschema, input_schema, execution_props)
        }
        Expr::IsUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsNotDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(&binary_op, input_dfschema, input_schema, execution_props)
        }
        Expr::IsNotUnknown(expr) => {
            let binary_op = binary_expr(
                expr.as_ref().clone(),
                Operator::IsDistinctFrom,
                Expr::Literal(ScalarValue::Boolean(None)),
            );
            create_physical_expr(&binary_op, input_dfschema, input_schema, execution_props)
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            // Create physical expressions for left and right operands
            let lhs = create_physical_expr(left, input_dfschema, input_schema, execution_props)?;
            let rhs = create_physical_expr(right, input_dfschema, input_schema, execution_props)?;
            // Note that the logical planner is responsible
            // for type coercion on the arguments (e.g. if one
            // argument was originally Int32 and one was
            // Int64 they will both be coerced to Int64).
            //
            // There should be no coercion during physical
            // planning.
            binary(lhs, *op, rhs, input_schema)
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if escape_char.is_some() {
                return Err(DataFusionError::Execution(
                    "LIKE does not support escape_char".to_string(),
                ));
            }
            let physical_expr =
                create_physical_expr(expr, input_dfschema, input_schema, execution_props)?;
            let physical_pattern =
                create_physical_expr(pattern, input_dfschema, input_schema, execution_props)?;
            like(
                *negated,
                *case_insensitive,
                physical_expr,
                physical_pattern,
                input_schema,
            )
        }
        Expr::Case(case) => {
            let expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = &case.expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            let when_expr = case
                .when_then_expr
                .iter()
                .map(|(w, _)| {
                    create_physical_expr(w.as_ref(), input_dfschema, input_schema, execution_props)
                })
                .collect::<Result<Vec<_>>>()?;
            let then_expr = case
                .when_then_expr
                .iter()
                .map(|(_, t)| {
                    create_physical_expr(t.as_ref(), input_dfschema, input_schema, execution_props)
                })
                .collect::<Result<Vec<_>>>()?;
            let when_then_expr: Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> = when_expr
                .iter()
                .zip(then_expr.iter())
                .map(|(w, t)| (w.clone(), t.clone()))
                .collect();
            let else_expr: Option<Arc<dyn PhysicalExpr>> = if let Some(e) = &case.else_expr {
                Some(create_physical_expr(
                    e.as_ref(),
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?)
            } else {
                None
            };
            Ok(expressions::case(expr, when_then_expr, else_expr)?)
        }
        Expr::Cast(Cast { expr, data_type }) => expressions::cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::TryCast(TryCast { expr, data_type }) => expressions::try_cast(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
            data_type.clone(),
        ),
        Expr::Not(expr) => expressions::not(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::Negative(expr) => expressions::negative(
            create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            input_schema,
        ),
        Expr::IsNull(expr) => expressions::is_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::IsNotNull(expr) => expressions::is_not_null(create_physical_expr(
            expr,
            input_dfschema,
            input_schema,
            execution_props,
        )?),
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let field = match field {
                GetFieldAccess::NamedStructField { name } => {
                    GetFieldAccessExpr::NamedStructField { name: name.clone() }
                }
                GetFieldAccess::ListIndex { key } => GetFieldAccessExpr::ListIndex {
                    key: create_physical_expr(key, input_dfschema, input_schema, execution_props)?,
                },
                GetFieldAccess::ListRange { start, stop } => GetFieldAccessExpr::ListRange {
                    start: create_physical_expr(
                        start,
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )?,
                    stop: create_physical_expr(
                        stop,
                        input_dfschema,
                        input_schema,
                        execution_props,
                    )?,
                },
            };
            Ok(Arc::new(GetIndexedFieldExpr::new(
                create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
                field,
            )))
        }

        Expr::ScalarFunction(ScalarFunction { fun, args }) => {
            let physical_args = args
                .iter()
                .map(|e| create_physical_expr(e, input_dfschema, input_schema, execution_props))
                .collect::<Result<Vec<_>>>()?;
            functions::create_physical_expr(fun, &physical_args, input_schema, execution_props)
        }
        Expr::ScalarUDF(ScalarUDF { fun, args }) => {
            println!("ScalarUDF: {:?}", fun);

            let mut physical_args = vec![];
            for e in args {
                physical_args.push(create_physical_expr(
                    e,
                    input_dfschema,
                    input_schema,
                    execution_props,
                )?);
            }
            // udfs with zero params expect null array as input
            if args.is_empty() {
                physical_args.push(Arc::new(Literal::new(ScalarValue::Null)));
            }
            udf::create_physical_expr(fun.clone().as_ref(), &physical_args, input_schema)
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let value_expr =
                create_physical_expr(expr, input_dfschema, input_schema, execution_props)?;
            let low_expr =
                create_physical_expr(low, input_dfschema, input_schema, execution_props)?;
            let high_expr =
                create_physical_expr(high, input_dfschema, input_schema, execution_props)?;

            // rewrite the between into the two binary operators
            let binary_expr = binary(
                binary(value_expr.clone(), Operator::GtEq, low_expr, input_schema)?,
                Operator::And,
                binary(value_expr.clone(), Operator::LtEq, high_expr, input_schema)?,
                input_schema,
            );

            if *negated {
                expressions::not(binary_expr?)
            } else {
                binary_expr
            }
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => match expr.as_ref() {
            Expr::Literal(ScalarValue::Utf8(None)) => {
                Ok(expressions::lit(ScalarValue::Boolean(None)))
            }
            _ => {
                let value_expr =
                    create_physical_expr(expr, input_dfschema, input_schema, execution_props)?;

                let list_exprs = list
                    .iter()
                    .map(|expr| {
                        create_physical_expr(expr, input_dfschema, input_schema, execution_props)
                    })
                    .collect::<Result<Vec<_>>>()?;
                expressions::in_list(value_expr, list_exprs, negated, input_schema)
            }
        },
        other => {
            not_impl_err!("Physical plan does not support logical expression {other:?}")
        }
    }
}

fn create_function_physical_name(fun: &str, distinct: bool, args: &[Expr]) -> Result<String> {
    let names: Vec<String> = args
        .iter()
        .map(|e| create_physical_name(e, false))
        .collect::<Result<_>>()?;

    let distinct_str = match distinct {
        true => "DISTINCT ",
        false => "",
    };
    Ok(format!("{}({}{})", fun, distinct_str, names.join(",")))
}

pub(super) fn physical_name(e: &Expr) -> Result<String> {
    create_physical_name(e, true)
}

fn create_physical_name(e: &Expr, is_first_expr: bool) -> Result<String> {
    match e {
        Expr::Column(c) => {
            if is_first_expr {
                Ok(c.name.clone())
            } else {
                Ok(c.flat_name())
            }
        }
        Expr::Alias(Alias { name, .. }) => Ok(name.clone()),
        Expr::ScalarVariable(_, variable_names) => Ok(variable_names.join(".")),
        Expr::Literal(value) => Ok(format!("{value:?}")),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = create_physical_name(left, false)?;
            let right = create_physical_name(right, false)?;
            Ok(format!("{left} {op} {right}"))
        }
        Expr::Case(case) => {
            let mut name = "CASE ".to_string();
            if let Some(e) = &case.expr {
                let _ = write!(name, "{e} ");
            }
            for (w, t) in &case.when_then_expr {
                let _ = write!(name, "WHEN {w} THEN {t} ");
            }
            if let Some(e) = &case.else_expr {
                let _ = write!(name, "ELSE {e} ");
            }
            name += "END";
            Ok(name)
        }
        Expr::Cast(Cast { expr, .. }) => {
            // CAST does not change the expression name
            create_physical_name(expr, false)
        }
        Expr::TryCast(TryCast { expr, .. }) => {
            // CAST does not change the expression name
            create_physical_name(expr, false)
        }
        Expr::Not(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("NOT {expr}"))
        }
        Expr::Negative(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("(- {expr})"))
        }
        Expr::IsNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NULL"))
        }
        Expr::IsNotNull(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT NULL"))
        }
        Expr::IsTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS TRUE"))
        }
        Expr::IsFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS FALSE"))
        }
        Expr::IsUnknown(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS UNKNOWN"))
        }
        Expr::IsNotTrue(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT TRUE"))
        }
        Expr::IsNotFalse(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT FALSE"))
        }
        Expr::IsNotUnknown(expr) => {
            let expr = create_physical_name(expr, false)?;
            Ok(format!("{expr} IS NOT UNKNOWN"))
        }
        Expr::GetIndexedField(GetIndexedField { expr, field }) => {
            let expr = create_physical_name(expr, false)?;
            let name = match field {
                GetFieldAccess::NamedStructField { name } => format!("{expr}[{name}]"),
                GetFieldAccess::ListIndex { key } => {
                    let key = create_physical_name(key, false)?;
                    format!("{expr}[{key}]")
                }
                GetFieldAccess::ListRange { start, stop } => {
                    let start = create_physical_name(start, false)?;
                    let stop = create_physical_name(stop, false)?;
                    format!("{expr}[{start}:{stop}]")
                }
            };

            Ok(name)
        }
        Expr::ScalarFunction(func) => {
            create_function_physical_name(&func.fun.to_string(), false, &func.args)
        }
        Expr::ScalarUDF(ScalarUDF { fun, args }) => {
            println!("ScalarUDF: {:?}", fun);

            create_function_physical_name(&fun.name, false, args)
        }
        Expr::WindowFunction(WindowFunction { fun, args, .. }) => {
            create_function_physical_name(&fun.to_string(), false, args)
        }
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            ..
        }) => create_function_physical_name(&fun.to_string(), *distinct, args),
        Expr::AggregateUDF(AggregateUDF {
            fun,
            args,
            filter,
            order_by,
        }) => {
            // TODO: Add support for filter and order by in AggregateUDF
            if filter.is_some() {
                return internal_err!("aggregate expression with filter is not supported");
            }
            if order_by.is_some() {
                return internal_err!("aggregate expression with order_by is not supported");
            }
            let mut names = Vec::with_capacity(args.len());
            for e in args {
                names.push(create_physical_name(e, false)?);
            }
            Ok(format!("{}({})", fun.name, names.join(",")))
        }
        Expr::GroupingSet(grouping_set) => match grouping_set {
            GroupingSet::Rollup(exprs) => Ok(format!(
                "ROLLUP ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::Cube(exprs) => Ok(format!(
                "CUBE ({})",
                exprs
                    .iter()
                    .map(|e| create_physical_name(e, false))
                    .collect::<Result<Vec<_>>>()?
                    .join(", ")
            )),
            GroupingSet::GroupingSets(lists_of_exprs) => {
                let mut strings = vec![];
                for exprs in lists_of_exprs {
                    let exprs_str = exprs
                        .iter()
                        .map(|e| create_physical_name(e, false))
                        .collect::<Result<Vec<_>>>()?
                        .join(", ");
                    strings.push(format!("({exprs_str})"));
                }
                Ok(format!("GROUPING SETS ({})", strings.join(", ")))
            }
        },

        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let list = list.iter().map(|expr| create_physical_name(expr, false));
            if *negated {
                Ok(format!("{expr} NOT IN ({list:?})"))
            } else {
                Ok(format!("{expr} IN ({list:?})"))
            }
        }
        Expr::Exists { .. } => {
            not_impl_err!("EXISTS is not yet supported in the physical plan")
        }
        Expr::InSubquery(_) => {
            not_impl_err!("IN subquery is not yet supported in the physical plan")
        }
        Expr::ScalarSubquery(_) => {
            not_impl_err!("Scalar subqueries are not yet supported in the physical plan")
        }
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let low = create_physical_name(low, false)?;
            let high = create_physical_name(high, false)?;
            if *negated {
                Ok(format!("{expr} NOT BETWEEN {low} AND {high}"))
            } else {
                Ok(format!("{expr} BETWEEN {low} AND {high}"))
            }
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let op_name = if *case_insensitive { "ILIKE" } else { "LIKE" };
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT {op_name} {pattern}{escape}"))
            } else {
                Ok(format!("{expr} {op_name} {pattern}{escape}"))
            }
        }
        Expr::SimilarTo(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive: _,
        }) => {
            let expr = create_physical_name(expr, false)?;
            let pattern = create_physical_name(pattern, false)?;
            let escape = if let Some(char) = escape_char {
                format!("CHAR '{char}'")
            } else {
                "".to_string()
            };
            if *negated {
                Ok(format!("{expr} NOT SIMILAR TO {pattern}{escape}"))
            } else {
                Ok(format!("{expr} SIMILAR TO {pattern}{escape}"))
            }
        }
        Expr::Sort { .. } => {
            internal_err!("Create physical name does not support sort expression")
        }
        Expr::Wildcard => internal_err!("Create physical name does not support wildcard"),
        Expr::QualifiedWildcard { .. } => {
            internal_err!("Create physical name does not support qualified wildcard")
        }
        Expr::Placeholder(_) => {
            internal_err!("Create physical name does not support placeholder")
        }
        Expr::OuterReferenceColumn(_, _) => {
            internal_err!("Create physical name does not support OuterReferenceColumn")
        }
    }
}

/// Expand and align a GROUPING SET expression.
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
///
/// This will take a list of grouping sets and ensure that each group is
/// properly aligned for the physical execution plan. We do this by
/// identifying all unique expression in each group and conforming each
/// group to the same set of expression types and ordering.
/// For example, if we have something like `GROUPING SETS ((a,b,c),(a),(b),(b,c))`
/// we would expand this to `GROUPING SETS ((a,b,c),(a,NULL,NULL),(NULL,b,NULL),(NULL,b,c))
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
pub(super) fn merge_grouping_set_physical_expr(
    grouping_sets: &[Vec<Expr>],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_groups = grouping_sets.len();
    let mut all_exprs: Vec<Expr> = vec![];
    let mut grouping_set_expr: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];
    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = vec![];

    for expr in grouping_sets.iter().flatten() {
        if !all_exprs.contains(expr) {
            all_exprs.push(expr.clone());

            grouping_set_expr.push(get_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);

            null_exprs.push(get_null_physical_expr_pair(
                expr,
                input_dfschema,
                input_schema,
                session_state,
            )?);
        }
    }

    let mut merged_sets: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    for expr_group in grouping_sets.iter() {
        let group: Vec<bool> = all_exprs
            .iter()
            .map(|expr| !expr_group.contains(expr))
            .collect();

        merged_sets.push(group)
    }

    Ok(PhysicalGroupBy::new(
        grouping_set_expr,
        null_exprs,
        merged_sets,
    ))
}

/// Expand and align a CUBE expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
pub(super) fn create_cube_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();
    let num_groups = num_of_exprs * num_of_exprs;

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(num_of_exprs);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?)
    }

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_groups);

    groups.push(vec![false; num_of_exprs]);

    for null_count in 1..=num_of_exprs {
        for null_idx in (0..num_of_exprs).combinations(null_count) {
            let mut next_group: Vec<bool> = vec![false; num_of_exprs];
            null_idx.into_iter().for_each(|i| next_group[i] = true);
            groups.push(next_group);
        }
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// Expand and align a ROLLUP expression. This is a special case of GROUPING SETS
/// (see <https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-GROUPING-SETS>)
pub(super) fn create_rollup_physical_expr(
    exprs: &[Expr],
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<PhysicalGroupBy> {
    let num_of_exprs = exprs.len();

    let mut null_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(num_of_exprs);
    let mut all_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(num_of_exprs);

    let mut groups: Vec<Vec<bool>> = Vec::with_capacity(num_of_exprs + 1);

    for expr in exprs {
        null_exprs.push(get_null_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?);

        all_exprs.push(get_physical_expr_pair(
            expr,
            input_dfschema,
            input_schema,
            session_state,
        )?)
    }

    for total in 0..=num_of_exprs {
        let mut group: Vec<bool> = Vec::with_capacity(num_of_exprs);

        for index in 0..num_of_exprs {
            if index < total {
                group.push(false);
            } else {
                group.push(true);
            }
        }

        groups.push(group)
    }

    Ok(PhysicalGroupBy::new(all_exprs, null_exprs, groups))
}

/// For a given logical expr, get a properly typed NULL ScalarValue physical expression
fn get_null_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    println!("get_null_physical_expr_pair: {:?}", expr);

    let physical_expr = create_physical_expr(
        expr,
        input_dfschema,
        input_schema,
        session_state.execution_props(),
    )?;
    let physical_name = physical_name(&expr.clone())?;

    let data_type = physical_expr.data_type(input_schema)?;
    let null_value: ScalarValue = (&data_type).try_into()?;

    let null_value = Literal::new(null_value);
    Ok((Arc::new(null_value), physical_name))
}

fn get_physical_expr_pair(
    expr: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    session_state: &SessionState,
) -> Result<(Arc<dyn PhysicalExpr>, String)> {
    println!("get_physical_expr_pair: {:?}", expr);

    let physical_expr = create_physical_expr(
        expr,
        input_dfschema,
        input_schema,
        session_state.execution_props(),
    )?;
    let physical_name = physical_name(expr)?;
    Ok((physical_expr, physical_name))
}

/// Check if window bounds are valid after schema information is available, and
/// window_frame bounds are casted to the corresponding column type.
/// queries like:
/// OVER (ORDER BY a RANGES BETWEEN 3 PRECEDING AND 5 PRECEDING)
/// OVER (ORDER BY a RANGES BETWEEN INTERVAL '3 DAY' PRECEDING AND '5 DAY' PRECEDING)  are rejected
pub fn is_window_valid(window_frame: &WindowFrame) -> bool {
    match (&window_frame.start_bound, &window_frame.end_bound) {
        (WindowFrameBound::Following(_), WindowFrameBound::Preceding(_))
        | (WindowFrameBound::Following(_), WindowFrameBound::CurrentRow)
        | (WindowFrameBound::CurrentRow, WindowFrameBound::Preceding(_)) => false,
        (WindowFrameBound::Preceding(lhs), WindowFrameBound::Preceding(rhs)) => {
            !rhs.is_null() && (lhs.is_null() || (lhs >= rhs))
        }
        (WindowFrameBound::Following(lhs), WindowFrameBound::Following(rhs)) => {
            !lhs.is_null() && (rhs.is_null() || (lhs <= rhs))
        }
        _ => true,
    }
}

/// Create a window expression with a name from a logical expression
pub fn create_window_expr_with_name(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    println!("create_window_expr_with_name: {:?}", e);

    let name = name.into();
    match e {
        Expr::WindowFunction(WindowFunction {
            fun,
            args,
            partition_by,
            order_by,
            window_frame,
        }) => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let partition_by = partition_by
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let order_by = order_by
                .iter()
                .map(|e| {
                    create_physical_sort_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            if !is_window_valid(window_frame) {
                return plan_err!(
                    "Invalid window frame: start bound ({}) cannot be larger than end bound ({})",
                    window_frame.start_bound,
                    window_frame.end_bound
                );
            }

            let window_frame = Arc::new(window_frame.clone());
            windows::create_window_expr(
                fun,
                name,
                &args,
                &partition_by,
                &order_by,
                window_frame,
                physical_input_schema,
            )
        }
        other => plan_err!("Invalid window expression '{other:?}'"),
    }
}

/// Create a window expression from a logical expression or an alias
pub fn create_window_expr(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<Arc<dyn WindowExpr>> {
    // unpack aliased logical expressions, e.g. "sum(col) over () as total"
    let (name, e) = match e {
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        _ => (e.display_name()?, e),
    };
    create_window_expr_with_name(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

type AggregateExprWithOptionalArgs = (
    Arc<dyn AggregateExpr>,
    // The filter clause, if any
    Option<Arc<dyn PhysicalExpr>>,
    // Ordering requirements, if any
    Option<Vec<PhysicalSortExpr>>,
);

/// Create an aggregate expression with a name from a logical expression
pub fn create_aggregate_expr_with_name_and_maybe_filter(
    e: &Expr,
    name: impl Into<String>,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    println!("create_aggregate_expr_with_name_and_maybe_filter: {:?}", e);
    match e {
        Expr::AggregateFunction(AggregateFunction {
            fun,
            distinct,
            args,
            filter,
            order_by,
        }) => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let filter = match filter {
                Some(e) => Some(create_physical_expr(
                    e,
                    logical_input_schema,
                    physical_input_schema,
                    execution_props,
                )?),
                None => None,
            };
            let order_by = match order_by {
                Some(e) => Some(
                    e.iter()
                        .map(|expr| {
                            create_physical_sort_expr(
                                expr,
                                logical_input_schema,
                                physical_input_schema,
                                execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                None => None,
            };
            let ordering_reqs = order_by.clone().unwrap_or(vec![]);
            let agg_expr = aggregates::create_aggregate_expr(
                fun,
                *distinct,
                &args,
                &ordering_reqs,
                physical_input_schema,
                name,
            )?;
            Ok((agg_expr, filter, order_by))
        }
        Expr::AggregateUDF(AggregateUDF {
            fun,
            args,
            filter,
            order_by,
        }) => {
            let args = args
                .iter()
                .map(|e| {
                    create_physical_expr(
                        e,
                        logical_input_schema,
                        physical_input_schema,
                        execution_props,
                    )
                })
                .collect::<Result<Vec<_>>>()?;

            let filter = match filter {
                Some(e) => Some(create_physical_expr(
                    e,
                    logical_input_schema,
                    physical_input_schema,
                    execution_props,
                )?),
                None => None,
            };
            let order_by = match order_by {
                Some(e) => Some(
                    e.iter()
                        .map(|expr| {
                            create_physical_sort_expr(
                                expr,
                                logical_input_schema,
                                physical_input_schema,
                                execution_props,
                            )
                        })
                        .collect::<Result<Vec<_>>>()?,
                ),
                None => None,
            };

            let agg_expr = udaf::create_aggregate_expr(fun, &args, physical_input_schema, name);
            Ok((agg_expr?, filter, order_by))
        }
        other => internal_err!("Invalid aggregate expression '{other:?}'"),
    }
}

/// Create an aggregate expression from a logical expression or an alias
pub fn create_aggregate_expr_and_maybe_filter(
    e: &Expr,
    logical_input_schema: &DFSchema,
    physical_input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<AggregateExprWithOptionalArgs> {
    // unpack (nested) aliased logical expressions, e.g. "sum(col) as total"
    let (name, e) = match e {
        Expr::Alias(Alias { expr, name, .. }) => (name.clone(), expr.as_ref()),
        _ => (physical_name(e)?, e),
    };

    create_aggregate_expr_with_name_and_maybe_filter(
        e,
        name,
        logical_input_schema,
        physical_input_schema,
        execution_props,
    )
}

/// Create a physical sort expression from a logical expression
pub fn create_physical_sort_expr(
    e: &Expr,
    input_dfschema: &DFSchema,
    input_schema: &Schema,
    execution_props: &ExecutionProps,
) -> Result<PhysicalSortExpr> {
    if let Expr::Sort(expr::Sort {
        expr,
        asc,
        nulls_first,
    }) = e
    {
        Ok(PhysicalSortExpr {
            expr: create_physical_expr(expr, input_dfschema, input_schema, execution_props)?,
            options: SortOptions {
                descending: !asc,
                nulls_first: *nulls_first,
            },
        })
    } else {
        internal_err!("Expects a sort expression")
    }
}
