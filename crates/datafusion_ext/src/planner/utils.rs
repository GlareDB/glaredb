// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! SQL Utility Functions

use std::collections::{HashMap, HashSet};

use datafusion::arrow::datatypes::{DataType, DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::utils::get_at_indices;
use datafusion::common::{plan_err, Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::{Alias, GroupingSet, WindowFunction};
use datafusion::logical_expr::utils::{expr_as_column_expr, find_column_exprs};
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::sql::TableReference;
use parser::sqlparser::ast::{
    ExceptSelectItem,
    ExcludeSelectItem,
    Ident,
    WildcardAdditionalOptions,
};

/// Make a best-effort attempt at resolving all columns in the expression tree
pub(crate) fn resolve_columns(expr: &Expr, plan: &LogicalPlan) -> Result<Expr> {
    expr.clone().transform_up(&|nested_expr| {
        match nested_expr {
            Expr::Column(col) => {
                let field = plan.schema().field_from_column(&col)?;
                Ok(Transformed::Yes(Expr::Column(field.qualified_column())))
            }
            _ => {
                // keep recursing
                Ok(Transformed::No(nested_expr))
            }
        }
    })
}

/// Rebuilds an `Expr` as a projection on top of a collection of `Expr`'s.
///
/// For example, the expression `a + b < 1` would require, as input, the 2
/// individual columns, `a` and `b`. But, if the base expressions already
/// contain the `a + b` result, then that may be used in lieu of the `a` and
/// `b` columns.
///
/// This is useful in the context of a query like:
///
/// SELECT a + b < 1 ... GROUP BY a + b
///
/// where post-aggregation, `a + b` need not be a projection against the
/// individual columns `a` and `b`, but rather it is a projection against the
/// `a + b` found in the GROUP BY.
pub(crate) fn rebase_expr(expr: &Expr, base_exprs: &[Expr], plan: &LogicalPlan) -> Result<Expr> {
    expr.clone().transform_up(&|nested_expr| {
        if base_exprs.contains(&nested_expr) {
            Ok(Transformed::Yes(expr_as_column_expr(&nested_expr, plan)?))
        } else {
            Ok(Transformed::No(nested_expr))
        }
    })
}

/// Determines if the set of `Expr`'s are a valid projection on the input
/// `Expr::Column`'s.
pub(crate) fn check_columns_satisfy_exprs(
    columns: &[Expr],
    exprs: &[Expr],
    message_prefix: &str,
) -> Result<()> {
    columns.iter().try_for_each(|c| match c {
        Expr::Column(_) => Ok(()),
        _ => Err(DataFusionError::Internal(
            "Expr::Column are required".to_string(),
        )),
    })?;
    let column_exprs = find_column_exprs(exprs);
    for e in &column_exprs {
        match e {
            Expr::GroupingSet(GroupingSet::Rollup(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, message_prefix)?;
                }
            }
            Expr::GroupingSet(GroupingSet::Cube(exprs)) => {
                for e in exprs {
                    check_column_satisfies_expr(columns, e, message_prefix)?;
                }
            }
            Expr::GroupingSet(GroupingSet::GroupingSets(lists_of_exprs)) => {
                for exprs in lists_of_exprs {
                    for e in exprs {
                        check_column_satisfies_expr(columns, e, message_prefix)?;
                    }
                }
            }
            _ => check_column_satisfies_expr(columns, e, message_prefix)?,
        }
    }
    Ok(())
}

fn check_column_satisfies_expr(columns: &[Expr], expr: &Expr, message_prefix: &str) -> Result<()> {
    if !columns.contains(expr) {
        return Err(DataFusionError::Plan(format!(
            "{}: Expression {:?} could not be resolved from available columns: {}",
            message_prefix,
            expr,
            columns
                .iter()
                .map(|e| format!("{e}"))
                .collect::<Vec<String>>()
                .join(", ")
        )));
    }
    Ok(())
}

/// Returns mapping of each alias (`String`) to the expression (`Expr`) it is
/// aliasing.
pub(crate) fn extract_aliases(exprs: &[Expr]) -> HashMap<String, Expr> {
    exprs
        .iter()
        .filter_map(|expr| match expr {
            Expr::Alias(Alias { expr, name, .. }) => Some((name.clone(), *expr.clone())),
            _ => None,
        })
        .collect::<HashMap<String, Expr>>()
}

/// Given an expression that's literal int encoding position, lookup the corresponding expression
/// in the select_exprs list, if the index is within the bounds and it is indeed a position literal;
/// Otherwise, return None
pub(crate) fn resolve_positions_to_exprs(expr: &Expr, select_exprs: &[Expr]) -> Option<Expr> {
    match expr {
        // sql_expr_to_logical_expr maps number to i64
        // https://github.com/apache/arrow-datafusion/blob/8d175c759e17190980f270b5894348dc4cff9bbf/datafusion/src/sql/planner.rs#L882-L887
        Expr::Literal(ScalarValue::Int64(Some(position)))
            if position > &0_i64 && position <= &(select_exprs.len() as i64) =>
        {
            let index = (position - 1) as usize;
            let select_expr = &select_exprs[index];
            Some(match select_expr {
                Expr::Alias(Alias { expr, .. }) => *expr.clone(),
                _ => select_expr.clone(),
            })
        }
        _ => None,
    }
}

/// Rebuilds an `Expr` with columns that refer to aliases replaced by the
/// alias' underlying `Expr`.
pub(crate) fn resolve_aliases_to_exprs(
    expr: &Expr,
    aliases: &HashMap<String, Expr>,
) -> Result<Expr> {
    expr.clone().transform_up(&|nested_expr| match nested_expr {
        Expr::Column(c) if c.relation.is_none() => {
            if let Some(aliased_expr) = aliases.get(&c.name) {
                Ok(Transformed::Yes(aliased_expr.clone()))
            } else {
                Ok(Transformed::No(Expr::Column(c)))
            }
        }
        _ => Ok(Transformed::No(nested_expr)),
    })
}

/// given a slice of window expressions sharing the same sort key, find their common partition
/// keys.
pub fn window_expr_common_partition_keys(window_exprs: &[Expr]) -> Result<&[Expr]> {
    let all_partition_keys = window_exprs
        .iter()
        .map(|expr| match expr {
            Expr::WindowFunction(WindowFunction { partition_by, .. }) => Ok(partition_by),
            Expr::Alias(Alias { expr, .. }) => match expr.as_ref() {
                Expr::WindowFunction(WindowFunction { partition_by, .. }) => Ok(partition_by),
                expr => Err(DataFusionError::Execution(format!(
                    "Impossibly got non-window expr {expr:?}"
                ))),
            },
            expr => Err(DataFusionError::Execution(format!(
                "Impossibly got non-window expr {expr:?}"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    let result = all_partition_keys
        .iter()
        .min_by_key(|s| s.len())
        .ok_or_else(|| DataFusionError::Execution("No window expressions found".to_owned()))?;
    Ok(result)
}

/// Returns a validated `DataType` for the specified precision and
/// scale
pub(crate) fn make_decimal_type(precision: Option<u64>, scale: Option<u64>) -> Result<DataType> {
    // postgres like behavior
    let (precision, scale) = match (precision, scale) {
        (Some(p), Some(s)) => (p as u8, s as i8),
        (Some(p), None) => (p as u8, 0),
        (None, Some(_)) => {
            return Err(DataFusionError::Internal(
                "Cannot specify only scale for decimal data type".to_string(),
            ))
        }
        (None, None) => (DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE),
    };

    // Arrow decimal is i128 meaning 38 maximum decimal digits
    if precision == 0 || precision > DECIMAL128_MAX_PRECISION || scale.unsigned_abs() > precision {
        Err(DataFusionError::Internal(format!(
            "Decimal(precision = {precision}, scale = {scale}) should satisfy `0 < precision <= 38`, and `scale <= precision`."
        )))
    } else {
        Ok(DataType::Decimal128(precision, scale))
    }
}

// Normalize an owned identifier to a lowercase string unless the identifier is quoted.
pub(crate) fn normalize_ident(id: Ident) -> String {
    match id.quote_style {
        Some(_) => id.value,
        None => id.value.to_ascii_lowercase(),
    }
}

/// Copied from https://github.com/GlareDB/arrow-datafusion/blob/bf6f83b3d228fb386f9b4b20c254fa58e2412660/datafusion/expr/src/utils.rs#L354
/// Returns all `Expr`s in the schema, except the `Column`s in the `columns_to_skip`
fn get_exprs_except_skipped(schema: &DFSchema, columns_to_skip: HashSet<Column>) -> Vec<Expr> {
    if columns_to_skip.is_empty() {
        schema
            .fields()
            .iter()
            .map(|f| Expr::Column(f.qualified_column()))
            .collect::<Vec<Expr>>()
    } else {
        schema
            .fields()
            .iter()
            .filter_map(|f| {
                let col = f.qualified_column();
                if !columns_to_skip.contains(&col) {
                    Some(Expr::Column(col))
                } else {
                    None
                }
            })
            .collect::<Vec<Expr>>()
    }
}

/// Copied from https://github.com/GlareDB/arrow-datafusion/blob/bf6f83b3d228fb386f9b4b20c254fa58e2412660/datafusion/expr/src/utils.rs#L381
/// Resolves an `Expr::Wildcard` to a collection of `Expr::Column`'s.
pub fn expand_wildcard(
    schema: &DFSchema,
    plan: &LogicalPlan,
    wildcard_options: Option<&WildcardAdditionalOptions>,
) -> Result<Vec<Expr>> {
    let using_columns = plan.using_columns()?;
    let mut columns_to_skip = using_columns
        .into_iter()
        // For each USING JOIN condition, only expand to one of each join column in projection
        .flat_map(|cols| {
            let mut cols = cols.into_iter().collect::<Vec<_>>();
            // sort join columns to make sure we consistently keep the same
            // qualified column
            cols.sort();
            let mut out_column_names: HashSet<String> = HashSet::new();
            cols.into_iter()
                .filter_map(|c| {
                    if out_column_names.contains(&c.name) {
                        Some(c)
                    } else {
                        out_column_names.insert(c.name);
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .collect::<HashSet<_>>();
    let excluded_columns = if let Some(WildcardAdditionalOptions {
        opt_exclude,
        opt_except,
        ..
    }) = wildcard_options
    {
        get_excluded_columns(opt_exclude.as_ref(), opt_except.as_ref(), schema, &None)?
    } else {
        vec![]
    };
    // Add each excluded `Column` to columns_to_skip
    columns_to_skip.extend(excluded_columns);
    Ok(get_exprs_except_skipped(schema, columns_to_skip))
}

/// Copied from https://github.com/GlareDB/arrow-datafusion/blob/bf6f83b3d228fb386f9b4b20c254fa58e2412660/datafusion/expr/src/utils.rs#L424
/// Resolves an `Expr::Wildcard` to a collection of qualified `Expr::Column`'s.
pub fn expand_qualified_wildcard(
    qualifier: &str,
    schema: &DFSchema,
    wildcard_options: Option<&WildcardAdditionalOptions>,
) -> Result<Vec<Expr>> {
    let qualifier = TableReference::from(qualifier);
    let qualified_indices = schema.fields_indices_with_qualified(&qualifier);
    let projected_func_dependencies = schema
        .functional_dependencies()
        .project_functional_dependencies(&qualified_indices, qualified_indices.len());
    let qualified_fields = get_at_indices(schema.fields(), &qualified_indices)?;
    if qualified_fields.is_empty() {
        return plan_err!("Invalid qualifier {qualifier}");
    }
    let qualified_schema =
        DFSchema::new_with_metadata(qualified_fields, schema.metadata().clone())?
            // We can use the functional dependencies as is, since it only stores indices:
            .with_functional_dependencies(projected_func_dependencies)?;
    let excluded_columns = if let Some(WildcardAdditionalOptions {
        opt_exclude,
        opt_except,
        ..
    }) = wildcard_options
    {
        get_excluded_columns(
            opt_exclude.as_ref(),
            opt_except.as_ref(),
            schema,
            &Some(qualifier),
        )?
    } else {
        vec![]
    };
    // Add each excluded `Column` to columns_to_skip
    let mut columns_to_skip = HashSet::new();
    columns_to_skip.extend(excluded_columns);
    Ok(get_exprs_except_skipped(&qualified_schema, columns_to_skip))
}

/// Copied from https://github.com/GlareDB/arrow-datafusion/blob/bf6f83b3d228fb386f9b4b20c254fa58e2412660/datafusion/expr/src/utils.rs#L314
/// Find excluded columns in the schema, if any
/// SELECT * EXCLUDE(col1, col2), would return `vec![col1, col2]`
fn get_excluded_columns(
    opt_exclude: Option<&ExcludeSelectItem>,
    opt_except: Option<&ExceptSelectItem>,
    schema: &DFSchema,
    qualifier: &Option<TableReference>,
) -> datafusion::common::Result<Vec<Column>> {
    let mut idents = vec![];
    if let Some(excepts) = opt_except {
        idents.push(&excepts.first_element);
        idents.extend(&excepts.additional_elements);
    }
    if let Some(exclude) = opt_exclude {
        match exclude {
            ExcludeSelectItem::Single(ident) => idents.push(ident),
            ExcludeSelectItem::Multiple(idents_inner) => idents.extend(idents_inner),
        }
    }
    // Excluded columns should be unique
    let n_elem = idents.len();
    let unique_idents = idents.into_iter().collect::<HashSet<_>>();
    // if HashSet size, and vector length are different, this means that some of the excluded columns
    // are not unique. In this case return error.
    if n_elem != unique_idents.len() {
        return plan_err!("EXCLUDE or EXCEPT contains duplicate column names");
    }

    let mut result = vec![];
    for ident in unique_idents.into_iter() {
        let col_name = ident.value.as_str();
        let field = if let Some(qualifier) = qualifier {
            schema.field_with_qualified_name(qualifier, col_name)?
        } else {
            schema.field_with_unqualified_name(col_name)?
        };
        result.push(field.qualified_column())
    }
    Ok(result)
}
