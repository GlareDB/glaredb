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

pub(crate) mod arrow_cast;
mod binary_op;
mod function;
mod grouping_set;
mod identifier;
mod order_by;
mod subquery;
mod substring;
mod unary_op;
mod value;

use crate::planner::{AsyncContextProvider, SqlQueryPlanner};
use async_recursion::async_recursion;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, DFSchema, DataFusionError, Result, ScalarValue};
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::expr::{InList, Placeholder};
use datafusion::logical_expr::{
    col, expr, lit, AggregateFunction, Between, BinaryExpr, BuiltinScalarFunction, Cast, Expr,
    ExprSchemable, GetFieldAccess, GetIndexedField, Like, Operator, TryCast,
};
use datafusion::sql::planner::PlannerContext;
use datafusion::sql::sqlparser::ast::{
    ArrayAgg, Expr as SQLExpr, Interval, JsonOperator, TrimWhereField, Value,
};
use datafusion::sql::sqlparser::parser::ParserError::ParserError;

impl<'a, S: AsyncContextProvider> SqlQueryPlanner<'a, S> {
    #[async_recursion]
    pub(crate) async fn sql_expr_to_logical_expr(
        &mut self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        enum StackEntry {
            SQLExpr(Box<SQLExpr>),
            Operator(Operator),
        }

        // Virtual stack machine to convert SQLExpr to Expr
        // This allows visiting the expr tree in a depth-first manner which
        // produces expressions in postfix notations, i.e. `a + b` => `a b +`.
        // See https://github.com/apache/arrow-datafusion/issues/1444
        let mut stack = vec![StackEntry::SQLExpr(Box::new(sql))];
        let mut eval_stack = vec![];

        while let Some(entry) = stack.pop() {
            match entry {
                StackEntry::SQLExpr(sql_expr) => {
                    match *sql_expr {
                        SQLExpr::BinaryOp { left, op, right } => {
                            // Note the order that we push the entries to the stack
                            // is important. We want to visit the left node first.
                            let op = self.parse_sql_binary_op(op)?;
                            stack.push(StackEntry::Operator(op));
                            stack.push(StackEntry::SQLExpr(right));
                            stack.push(StackEntry::SQLExpr(left));
                        }
                        _ => {
                            let expr = self
                                .sql_expr_to_logical_expr_internal(
                                    *sql_expr,
                                    schema,
                                    planner_context,
                                )
                                .await?;
                            eval_stack.push(expr);
                        }
                    }
                }
                StackEntry::Operator(op) => {
                    let right = eval_stack.pop().unwrap();
                    let left = eval_stack.pop().unwrap();
                    let expr =
                        Expr::BinaryExpr(BinaryExpr::new(Box::new(left), op, Box::new(right)));
                    eval_stack.push(expr);
                }
            }
        }

        assert_eq!(1, eval_stack.len());
        let expr = eval_stack.pop().unwrap();
        Ok(expr)
    }

    /// Generate a relational expression from a SQL expression
    pub async fn sql_to_expr(
        &mut self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut expr = self
            .sql_expr_to_logical_expr(sql, schema, planner_context)
            .await?;
        expr = self.rewrite_partial_qualifier(expr, schema);
        self.validate_schema_satisfies_exprs(schema, &[expr.clone()])?;
        let expr = infer_placeholder_types(expr, schema)?;
        Ok(expr)
    }

    /// Rewrite aliases which are not-complete (e.g. ones that only include only table qualifier in a schema.table qualified relation)
    fn rewrite_partial_qualifier(&self, expr: Expr, schema: &DFSchema) -> Expr {
        match expr {
            Expr::Column(col) => match &col.relation {
                Some(q) => {
                    match schema
                        .fields()
                        .iter()
                        .find(|field| match field.qualifier() {
                            Some(field_q) => {
                                field.name() == &col.name
                                    && field_q.to_string().ends_with(&format!(".{q}"))
                            }
                            _ => false,
                        }) {
                        Some(df_field) => Expr::Column(Column {
                            relation: df_field.qualifier().cloned(),
                            name: df_field.name().clone(),
                        }),
                        None => Expr::Column(col),
                    }
                }
                None => Expr::Column(col),
            },
            _ => expr,
        }
    }

    /// Internal implementation. Use
    /// [`Self::sql_expr_to_logical_expr`] to plan exprs.
    async fn sql_expr_to_logical_expr_internal(
        &mut self,
        sql: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match sql {
            SQLExpr::Value(value) => {
                self.parse_value(value, planner_context.prepare_param_data_types())
            }
            SQLExpr::Extract { field, expr } => Ok(Expr::ScalarFunction(ScalarFunction::new(
                BuiltinScalarFunction::DatePart,
                vec![
                    Expr::Literal(ScalarValue::Utf8(Some(format!("{field}")))),
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                        .await?,
                ],
            ))),

            SQLExpr::Array(arr) => self.sql_array_literal(arr.elem, schema).await,
            SQLExpr::Interval(Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            }) => {
                self.sql_interval_to_expr(
                    *value,
                    schema,
                    planner_context,
                    leading_field,
                    leading_precision,
                    last_field,
                    fractional_seconds_precision,
                )
                .await
            }
            SQLExpr::Identifier(id) => {
                self.sql_identifier_to_expr(id, schema, planner_context)
                    .await
            }

            SQLExpr::MapAccess { column, keys } => {
                if let SQLExpr::Identifier(id) = *column {
                    self.plan_indexed(
                        col(self.normalizer.normalize(id)),
                        keys,
                        schema,
                        planner_context,
                    )
                    .await
                } else {
                    Err(DataFusionError::NotImplemented(format!(
                        "map access requires an identifier, found column {column} instead"
                    )))
                }
            }

            SQLExpr::ArrayIndex { obj, indexes } => {
                let expr = self
                    .sql_expr_to_logical_expr(*obj, schema, planner_context)
                    .await?;
                self.plan_indexed(expr, indexes, schema, planner_context)
                    .await
            }

            SQLExpr::CompoundIdentifier(ids) => {
                self.sql_compound_identifier_to_expr(ids, schema, planner_context)
                    .await
            }

            SQLExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => {
                self.sql_case_identifier_to_expr(
                    operand,
                    conditions,
                    results,
                    else_result,
                    schema,
                    planner_context,
                )
                .await
            }

            SQLExpr::Cast { expr, data_type } => Ok(Expr::Cast(Cast::new(
                Box::new(
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                        .await?,
                ),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::TryCast { expr, data_type } => Ok(Expr::TryCast(TryCast::new(
                Box::new(
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                        .await?,
                ),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::TypedString { data_type, value } => Ok(Expr::Cast(Cast::new(
                Box::new(lit(value)),
                self.convert_data_type(&data_type)?,
            ))),

            SQLExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsDistinctFrom(left, right) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(
                    self.sql_expr_to_logical_expr(*left, schema, planner_context)
                        .await?,
                ),
                Operator::IsDistinctFrom,
                Box::new(
                    self.sql_expr_to_logical_expr(*right, schema, planner_context)
                        .await?,
                ),
            ))),

            SQLExpr::IsNotDistinctFrom(left, right) => Ok(Expr::BinaryExpr(BinaryExpr::new(
                Box::new(
                    self.sql_expr_to_logical_expr(*left, schema, planner_context)
                        .await?,
                ),
                Operator::IsNotDistinctFrom,
                Box::new(
                    self.sql_expr_to_logical_expr(*right, schema, planner_context)
                        .await?,
                ),
            ))),

            SQLExpr::IsTrue(expr) => Ok(Expr::IsTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsFalse(expr) => Ok(Expr::IsFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsNotTrue(expr) => Ok(Expr::IsNotTrue(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsNotFalse(expr) => Ok(Expr::IsNotFalse(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsUnknown(expr) => Ok(Expr::IsUnknown(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::IsNotUnknown(expr) => Ok(Expr::IsNotUnknown(Box::new(
                self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                    .await?,
            ))),

            SQLExpr::UnaryOp { op, expr } => {
                self.parse_sql_unary_op(op, *expr, schema, planner_context)
                    .await
            }

            SQLExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between(Between::new(
                Box::new(
                    self.sql_expr_to_logical_expr(*expr, schema, planner_context)
                        .await?,
                ),
                negated,
                Box::new(
                    self.sql_expr_to_logical_expr(*low, schema, planner_context)
                        .await?,
                ),
                Box::new(
                    self.sql_expr_to_logical_expr(*high, schema, planner_context)
                        .await?,
                ),
            ))),

            SQLExpr::InList {
                expr,
                list,
                negated,
            } => {
                self.sql_in_list_to_expr(*expr, list, negated, schema, planner_context)
                    .await
            }

            SQLExpr::Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                self.sql_like_to_expr(
                    negated,
                    *expr,
                    *pattern,
                    escape_char,
                    schema,
                    planner_context,
                    false,
                )
                .await
            }

            SQLExpr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                self.sql_like_to_expr(
                    negated,
                    *expr,
                    *pattern,
                    escape_char,
                    schema,
                    planner_context,
                    true,
                )
                .await
            }

            SQLExpr::SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => {
                self.sql_similarto_to_expr(
                    negated,
                    *expr,
                    *pattern,
                    escape_char,
                    schema,
                    planner_context,
                )
                .await
            }

            SQLExpr::BinaryOp { .. } => Err(DataFusionError::Internal(
                "binary_op should be handled by sql_expr_to_logical_expr.".to_string(),
            )),

            SQLExpr::Substring {
                expr,
                substring_from,
                substring_for,
                special: false,
            } => {
                self.sql_substring_to_expr(
                    expr,
                    substring_from,
                    substring_for,
                    schema,
                    planner_context,
                )
                .await
            }

            SQLExpr::Trim {
                expr,
                trim_where,
                trim_what,
            } => {
                self.sql_trim_to_expr(*expr, trim_where, trim_what, schema, planner_context)
                    .await
            }

            SQLExpr::AggregateExpressionWithFilter { expr, filter } => {
                self.sql_agg_with_filter_to_expr(*expr, *filter, schema, planner_context)
                    .await
            }

            SQLExpr::Function(function) => {
                self.sql_function_to_expr(function, schema, planner_context)
                    .await
            }

            SQLExpr::Rollup(exprs) => {
                self.sql_rollup_to_expr(exprs, schema, planner_context)
                    .await
            }
            SQLExpr::Cube(exprs) => self.sql_cube_to_expr(exprs, schema, planner_context).await,
            SQLExpr::GroupingSets(exprs) => {
                self.sql_grouping_sets_to_expr(exprs, schema, planner_context)
                    .await
            }

            SQLExpr::Floor {
                expr,
                field: _field,
            } => {
                self.sql_named_function_to_expr(
                    *expr,
                    BuiltinScalarFunction::Floor,
                    schema,
                    planner_context,
                )
                .await
            }
            SQLExpr::Ceil {
                expr,
                field: _field,
            } => {
                self.sql_named_function_to_expr(
                    *expr,
                    BuiltinScalarFunction::Ceil,
                    schema,
                    planner_context,
                )
                .await
            }

            SQLExpr::Nested(e) => {
                self.sql_expr_to_logical_expr(*e, schema, planner_context)
                    .await
            }

            SQLExpr::Exists { subquery, negated } => {
                self.parse_exists_subquery(*subquery, negated, schema, planner_context)
                    .await
            }
            SQLExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                self.parse_in_subquery(*expr, *subquery, negated, schema, planner_context)
                    .await
            }
            SQLExpr::Subquery(subquery) => {
                self.parse_scalar_subquery(*subquery, schema, planner_context)
                    .await
            }

            SQLExpr::ArrayAgg(array_agg) => {
                self.parse_array_agg(array_agg, schema, planner_context)
                    .await
            }

            _ => Err(DataFusionError::NotImplemented(format!(
                "Unsupported ast node in sqltorel: {sql:?}"
            ))),
        }
    }

    async fn parse_array_agg(
        &mut self,
        array_agg: ArrayAgg,
        input_schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        // Some dialects have special syntax for array_agg. DataFusion only supports it like a function.
        let ArrayAgg {
            distinct,
            expr,
            order_by,
            limit,
            within_group,
        } = array_agg;

        let order_by = if let Some(order_by) = order_by {
            Some(
                self.order_by_to_sort_expr(&order_by, input_schema, planner_context)
                    .await?,
            )
        } else {
            None
        };

        if let Some(limit) = limit {
            return Err(DataFusionError::NotImplemented(format!(
                "LIMIT not supported in ARRAY_AGG: {limit}"
            )));
        }

        if within_group {
            return Err(DataFusionError::NotImplemented(
                "WITHIN GROUP not supported in ARRAY_AGG".to_string(),
            ));
        }

        let args = vec![
            self.sql_expr_to_logical_expr(*expr, input_schema, planner_context)
                .await?,
        ];

        // next, aggregate built-ins
        let fun = AggregateFunction::ArrayAgg;
        Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
            fun, args, distinct, None, order_by,
        )))
    }

    async fn sql_in_list_to_expr(
        &mut self,
        expr: SQLExpr,
        list: Vec<SQLExpr>,
        negated: bool,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let mut list_expr = Vec::with_capacity(list.len());
        for e in list {
            let e = self
                .sql_expr_to_logical_expr(e, schema, planner_context)
                .await?;
            list_expr.push(e);
        }

        Ok(Expr::InList(InList::new(
            Box::new(
                self.sql_expr_to_logical_expr(expr, schema, planner_context)
                    .await?,
            ),
            list_expr,
            negated,
        )))
    }

    #[allow(clippy::too_many_arguments)]
    async fn sql_like_to_expr(
        &mut self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<char>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
        case_insensitive: bool,
    ) -> Result<Expr> {
        let pattern = self
            .sql_expr_to_logical_expr(pattern, schema, planner_context)
            .await?;
        let pattern_type = pattern.get_type(schema)?;
        if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
            return Err(DataFusionError::Plan(
                "Invalid pattern in LIKE expression".to_string(),
            ));
        }
        Ok(Expr::Like(Like::new(
            negated,
            Box::new(
                self.sql_expr_to_logical_expr(expr, schema, planner_context)
                    .await?,
            ),
            Box::new(pattern),
            escape_char,
            case_insensitive,
        )))
    }

    async fn sql_similarto_to_expr(
        &mut self,
        negated: bool,
        expr: SQLExpr,
        pattern: SQLExpr,
        escape_char: Option<char>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let pattern = self
            .sql_expr_to_logical_expr(pattern, schema, planner_context)
            .await?;
        let pattern_type = pattern.get_type(schema)?;
        if pattern_type != DataType::Utf8 && pattern_type != DataType::Null {
            return Err(DataFusionError::Plan(
                "Invalid pattern in SIMILAR TO expression".to_string(),
            ));
        }
        Ok(Expr::SimilarTo(Like::new(
            negated,
            Box::new(
                self.sql_expr_to_logical_expr(expr, schema, planner_context)
                    .await?,
            ),
            Box::new(pattern),
            escape_char,
            false,
        )))
    }

    async fn sql_trim_to_expr(
        &mut self,
        expr: SQLExpr,
        trim_where: Option<TrimWhereField>,
        trim_what: Option<Box<SQLExpr>>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let fun = match trim_where {
            Some(TrimWhereField::Leading) => BuiltinScalarFunction::Ltrim,
            Some(TrimWhereField::Trailing) => BuiltinScalarFunction::Rtrim,
            Some(TrimWhereField::Both) => BuiltinScalarFunction::Btrim,
            None => BuiltinScalarFunction::Trim,
        };
        let arg = self
            .sql_expr_to_logical_expr(expr, schema, planner_context)
            .await?;
        let args = match trim_what {
            Some(to_trim) => {
                let to_trim = self
                    .sql_expr_to_logical_expr(*to_trim, schema, planner_context)
                    .await?;
                vec![arg, to_trim]
            }
            None => vec![arg],
        };
        Ok(Expr::ScalarFunction(ScalarFunction::new(fun, args)))
    }

    async fn sql_agg_with_filter_to_expr(
        &mut self,
        expr: SQLExpr,
        filter: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        match self
            .sql_expr_to_logical_expr(expr, schema, planner_context)
            .await?
        {
            Expr::AggregateFunction(expr::AggregateFunction {
                fun,
                args,
                distinct,
                order_by,
                ..
            }) => Ok(Expr::AggregateFunction(expr::AggregateFunction::new(
                fun,
                args,
                distinct,
                Some(Box::new(
                    self.sql_expr_to_logical_expr(filter, schema, planner_context)
                        .await?,
                )),
                order_by,
            ))),
            _ => Err(DataFusionError::Internal(
                "AggregateExpressionWithFilter expression was not an AggregateFunction".to_string(),
            )),
        }
    }

    async fn plan_indices(
        &mut self,
        expr: SQLExpr,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<GetFieldAccess> {
        let field = match expr.clone() {
            SQLExpr::Value(Value::SingleQuotedString(s) | Value::DoubleQuotedString(s)) => {
                GetFieldAccess::NamedStructField {
                    name: ScalarValue::Utf8(Some(s)),
                }
            }
            SQLExpr::JsonAccess {
                left,
                operator: JsonOperator::Colon,
                right,
            } => {
                let start = Box::new(
                    self.sql_expr_to_logical_expr(*left, schema, planner_context)
                        .await?,
                );
                let stop = Box::new(
                    self.sql_expr_to_logical_expr(*right, schema, planner_context)
                        .await?,
                );

                GetFieldAccess::ListRange { start, stop }
            }
            _ => GetFieldAccess::ListIndex {
                key: Box::new(
                    self.sql_expr_to_logical_expr(expr, schema, planner_context)
                        .await?,
                ),
            },
        };

        Ok(field)
    }

    #[async_recursion]
    async fn plan_indexed(
        &mut self,
        expr: Expr,
        mut keys: Vec<SQLExpr>,
        schema: &DFSchema,
        planner_context: &mut PlannerContext,
    ) -> Result<Expr> {
        let indices = keys.pop().ok_or_else(|| {
            ParserError("Internal error: Missing index key expression".to_string())
        })?;

        let expr = if !keys.is_empty() {
            self.plan_indexed(expr, keys, schema, planner_context)
                .await?
        } else {
            expr
        };

        Ok(Expr::GetIndexedField(GetIndexedField::new(
            Box::new(expr),
            self.plan_indices(indices, schema, planner_context).await?,
        )))
    }
}

// modifies expr if it is a placeholder with datatype of right
fn rewrite_placeholder(expr: &mut Expr, other: &Expr, schema: &DFSchema) -> Result<()> {
    if let Expr::Placeholder(Placeholder { id: _, data_type }) = expr {
        if data_type.is_none() {
            let other_dt = other.get_type(schema);
            match other_dt {
                Err(e) => {
                    Err(e.context(format!(
                        "Can not find type of {other} needed to infer type of {expr}"
                    )))?;
                }
                Ok(dt) => {
                    *data_type = Some(dt);
                }
            }
        };
    }
    Ok(())
}

/// Find all [`Expr::Placeholder`] tokens in a logical plan, and try
/// to infer their [`DataType`] from the context of their use.
fn infer_placeholder_types(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    expr.transform(&|mut expr| {
        // Default to assuming the arguments are the same type
        if let Expr::BinaryExpr(BinaryExpr { left, op: _, right }) = &mut expr {
            rewrite_placeholder(left.as_mut(), right.as_ref(), schema)?;
            rewrite_placeholder(right.as_mut(), left.as_ref(), schema)?;
        };
        Ok(Transformed::Yes(expr))
    })
}
