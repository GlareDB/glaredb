use fmtutil::IntoDisplayableSlice;
use rayexec_error::{not_implemented, RayexecError, Result};
use rayexec_parser::ast::{self, QueryNode};

use super::bind_context::{BindContext, BindScopeRef};
use super::column_binder::ExpressionColumnBinder;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::interval::Interval;
use crate::arrays::scalar::{BorrowedScalarValue, ScalarValue};
use crate::expr::aggregate_expr::AggregateExpr;
use crate::expr::arith_expr::ArithOperator;
use crate::expr::case_expr::{CaseExpr, WhenThen};
use crate::expr::cast_expr::CastExpr;
use crate::expr::comparison_expr::{ComparisonExpr, ComparisonOperator};
use crate::expr::conjunction_expr::{ConjunctionExpr, ConjunctionOperator};
use crate::expr::grouping_set_expr::GroupingSetExpr;
use crate::expr::literal_expr::LiteralExpr;
use crate::expr::negate_expr::{NegateExpr, NegateOperator};
use crate::expr::scalar_function_expr::ScalarFunctionExpr;
use crate::expr::subquery_expr::{SubqueryExpr, SubqueryType};
use crate::expr::unnest_expr::UnnestExpr;
use crate::expr::window_expr::{WindowExpr, WindowFrameBound, WindowFrameExclusion};
use crate::expr::{self, bind_aggregate_function, AsScalarFunctionSet, Expression};
use crate::functions::aggregate::AggregateFunction2;
use crate::functions::candidate::CastType;
use crate::functions::scalar::builtin::datetime::{DatePart, FUNCTION_SET_DATE_PART};
use crate::functions::scalar::builtin::is::{
    FUNCTION_SET_IS_FALSE,
    FUNCTION_SET_IS_NOT_FALSE,
    FUNCTION_SET_IS_NOT_NULL,
    FUNCTION_SET_IS_NOT_TRUE,
    FUNCTION_SET_IS_NULL,
    FUNCTION_SET_IS_TRUE,
};
use crate::functions::scalar::builtin::list::{
    FUNCTION_SET_LIST_EXTRACT,
    FUNCTION_SET_LIST_VALUES,
};
use crate::functions::scalar::builtin::string::{
    FUNCTION_SET_CONCAT,
    FUNCTION_SET_LIKE,
    FUNCTION_SET_STARTS_WITH,
    FUNCTION_SET_SUBSTRING,
};
use crate::functions::table::TableFunction;
use crate::logical::binder::bind_query::bind_modifier::BoundOrderByExpr;
use crate::logical::binder::bind_query::QueryBinder;
use crate::logical::resolver::resolve_context::ResolveContext;
use crate::logical::resolver::resolved_function::{ResolvedFunction, SpecialBuiltinFunction};
use crate::logical::resolver::ResolvedMeta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecursionContext {
    /// Whether to allow aggregate function binding.
    pub allow_aggregates: bool,
    /// Whether to allow window function binding.
    pub allow_windows: bool,
    /// If we're in the root expression.
    pub is_root: bool,
}

impl RecursionContext {
    fn not_root(self) -> Self {
        RecursionContext {
            is_root: false,
            ..self
        }
    }
}

#[derive(Debug)]
pub struct BaseExpressionBinder<'a> {
    pub current: BindScopeRef,
    pub resolve_context: &'a ResolveContext,
}

impl<'a> BaseExpressionBinder<'a> {
    pub const fn new(current: BindScopeRef, resolve_context: &'a ResolveContext) -> Self {
        BaseExpressionBinder {
            current,
            resolve_context,
        }
    }

    pub fn bind_expressions(
        &self,
        bind_context: &mut BindContext,
        exprs: &[ast::Expr<ResolvedMeta>],
        column_binder: &mut impl ExpressionColumnBinder,
        recur: RecursionContext,
    ) -> Result<Vec<Expression>> {
        exprs
            .iter()
            .map(|expr| self.bind_expression(bind_context, expr, column_binder, recur))
            .collect::<Result<Vec<_>>>()
    }

    pub fn bind_expression(
        &self,
        bind_context: &mut BindContext,
        expr: &ast::Expr<ResolvedMeta>,
        column_binder: &mut impl ExpressionColumnBinder,
        recur: RecursionContext,
    ) -> Result<Expression> {
        // check_stack_redline("bind expression")?;

        match expr {
            ast::Expr::Ident(ident) => {
                // Use the provided column binder, no fallback.
                match column_binder.bind_from_ident(self.current, bind_context, ident, recur)? {
                    Some(expr) => Ok(expr),
                    None => Err(RayexecError::new(format!(
                        "Missing column for reference: {ident}",
                    ))),
                }
            }
            ast::Expr::CompoundIdent(idents) => {
                // Use the provided column binder, no fallback.
                match column_binder.bind_from_idents(self.current, bind_context, idents, recur)? {
                    Some(expr) => Ok(expr),
                    None => {
                        let ident_string = idents
                            .iter()
                            .map(|i| i.as_normalized_string())
                            .collect::<Vec<_>>()
                            .join(".");
                        Err(RayexecError::new(format!(
                            "Missing column for reference: {ident_string}",
                        )))
                    }
                }
            }
            ast::Expr::QualifiedWildcard(_) => Err(RayexecError::new(
                "Qualified wildcard not a valid expression to bind",
            )),
            ast::Expr::Literal(literal) => {
                // Use the provided column binder only if this is the root of
                // the expression.
                //
                // Fallback to normal literal binding.
                if recur.is_root {
                    if let Some(expr) =
                        column_binder.bind_from_root_literal(self.current, bind_context, literal)?
                    {
                        return Ok(expr);
                    }
                }
                Self::bind_literal(literal)
            }
            ast::Expr::Array(arr) => {
                let exprs = arr
                    .iter()
                    .map(|v| {
                        self.bind_expression(
                            bind_context,
                            v,
                            column_binder,
                            RecursionContext {
                                is_root: false,
                                ..recur
                            },
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;

                let function = expr::bind_scalar_function(&FUNCTION_SET_LIST_VALUES, exprs)?;

                Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
            }
            ast::Expr::ArraySubscript { expr, subscript } => {
                let expr = self.bind_expression(
                    bind_context,
                    expr.as_ref(),
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;
                match subscript.as_ref() {
                    ast::ArraySubscript::Index(index) => {
                        let index = self.bind_expression(
                            bind_context,
                            index,
                            column_binder,
                            RecursionContext {
                                allow_windows: false,
                                allow_aggregates: false,
                                is_root: false,
                            },
                        )?;

                        let function = expr::bind_scalar_function(
                            &FUNCTION_SET_LIST_EXTRACT,
                            vec![expr, index],
                        )?;

                        Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
                    }
                    ast::ArraySubscript::Slice { .. } => {
                        Err(RayexecError::new("Array slicing not yet implemented"))
                    }
                }
            }
            ast::Expr::UnaryExpr { op, expr } => {
                let expr = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                Ok(match op {
                    ast::UnaryOperator::Plus => expr,
                    ast::UnaryOperator::Not => expr::negate(NegateOperator::Not, expr)?.into(),
                    ast::UnaryOperator::Minus => expr::negate(NegateOperator::Negate, expr)?.into(),
                })
            }
            ast::Expr::BinaryExpr { left, op, right } => {
                let left = self.bind_expression(
                    bind_context,
                    left,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;
                let right = self.bind_expression(
                    bind_context,
                    right,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let table_list = bind_context.get_table_list();

                Ok(match op {
                    ast::BinaryOperator::NotEq => {
                        let op = ComparisonOperator::NotEq;
                        expr::compare(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Eq => {
                        let op = ComparisonOperator::Eq;
                        expr::compare(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Lt => {
                        let op = ComparisonOperator::Lt;
                        expr::compare(op, left, right)?.into()
                    }
                    ast::BinaryOperator::LtEq => {
                        let op = ComparisonOperator::LtEq;
                        expr::compare(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Gt => {
                        let op = ComparisonOperator::Gt;
                        expr::compare(op, left, right)?.into()
                    }
                    ast::BinaryOperator::GtEq => {
                        let op = ComparisonOperator::GtEq;
                        expr::compare(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Plus => {
                        let op = ArithOperator::Add;
                        expr::arith(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Minus => {
                        let op = ArithOperator::Sub;
                        expr::arith(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Multiply => {
                        let op = ArithOperator::Mul;
                        expr::arith(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Divide => {
                        let op = ArithOperator::Div;
                        expr::arith(op, left, right)?.into()
                    }
                    ast::BinaryOperator::Modulo => {
                        let op = ArithOperator::Mod;
                        expr::arith(op, left, right)?.into()
                    }
                    ast::BinaryOperator::And => {
                        let op = ConjunctionOperator::And;
                        expr::conjunction(op, [left, right])?.into()
                    }
                    ast::BinaryOperator::Or => {
                        let op = ConjunctionOperator::Or;
                        expr::conjunction(op, [left, right])?.into()
                    }
                    ast::BinaryOperator::StringConcat => {
                        let function =
                            expr::bind_scalar_function(&FUNCTION_SET_CONCAT, vec![left, right])?;
                        Expression::ScalarFunction(ScalarFunctionExpr { function })
                    }
                    ast::BinaryOperator::StringStartsWith => {
                        let function = expr::bind_scalar_function(
                            &FUNCTION_SET_STARTS_WITH,
                            vec![left, right],
                        )?;
                        Expression::ScalarFunction(ScalarFunctionExpr { function })
                    }
                    other => not_implemented!("binary operator {other:?}"),
                })
            }
            ast::Expr::Function(func) => {
                self.bind_function(bind_context, func, column_binder, recur)
            }
            ast::Expr::Nested(nested) => self.bind_expression(
                bind_context,
                nested,
                column_binder,
                RecursionContext {
                    is_root: false,
                    ..recur
                },
            ),
            ast::Expr::Subquery(subquery) => {
                self.bind_subquery(bind_context, subquery, SubqueryType::Scalar)
            }
            ast::Expr::Tuple(_) => not_implemented!("tuple expressions"),
            ast::Expr::Collate { .. } => not_implemented!("COLLATE"),
            ast::Expr::Exists {
                subquery,
                not_exists,
            } => self.bind_subquery(
                bind_context,
                subquery,
                SubqueryType::Exists {
                    negated: *not_exists,
                },
            ),
            ast::Expr::AnySubquery { left, op, right }
            | ast::Expr::AllSubquery { left, op, right } => {
                let bound_expr = self.bind_expression(
                    bind_context,
                    left,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let op = match op {
                    ast::BinaryOperator::Eq => ComparisonOperator::Eq,
                    ast::BinaryOperator::NotEq => ComparisonOperator::NotEq,
                    ast::BinaryOperator::Gt => ComparisonOperator::Gt,
                    ast::BinaryOperator::GtEq => ComparisonOperator::GtEq,
                    ast::BinaryOperator::Lt => ComparisonOperator::Lt,
                    ast::BinaryOperator::LtEq => ComparisonOperator::LtEq,
                    _ => {
                        return Err(RayexecError::new(
                            "ANY/ALL can only have =, <>, <, >, <=, or >= as an operator",
                        ))
                    }
                };

                match expr {
                    ast::Expr::AnySubquery { .. } => {
                        let typ = SubqueryType::Any {
                            expr: Box::new(bound_expr),
                            op,
                        };
                        self.bind_subquery(bind_context, right, typ)
                    }
                    ast::Expr::AllSubquery { .. } => {
                        // Negate the op, and negate the resulting sbuquery expr.
                        // '= ALL(..)' => 'NOT(<> ANY(..))'
                        let typ = SubqueryType::Any {
                            expr: Box::new(bound_expr),
                            op: op.negate(),
                        };
                        let subquery = self.bind_subquery(bind_context, right, typ)?;

                        let negated = expr::negate(NegateOperator::Not, subquery)?;

                        Ok(negated.into())
                    }
                    _ => unreachable!(),
                }
            }
            ast::Expr::InSubquery {
                negated,
                expr,
                subquery,
            } => {
                let bound_expr = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let mut expr = self.bind_subquery(
                    bind_context,
                    subquery,
                    SubqueryType::Any {
                        expr: Box::new(bound_expr),
                        op: ComparisonOperator::Eq,
                    },
                )?;

                if *negated {
                    expr = expr::negate(NegateOperator::Not, expr)?.into();
                }

                Ok(expr)
            }
            ast::Expr::InList {
                negated,
                expr,
                list,
            } => {
                let needle = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let list = self.bind_expressions(
                    bind_context,
                    list,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                // 'IN (..)' => '(needle = a OR needle = b ...))'
                // 'NOT IN (..)' => '(needle <> a AND needle <> b ...))'
                let (conj_op, cmp_op) = if !negated {
                    (ConjunctionOperator::Or, ComparisonOperator::Eq)
                } else {
                    (ConjunctionOperator::And, ComparisonOperator::NotEq)
                };

                let cmp_exprs = list
                    .into_iter()
                    .map(|expr| {
                        let cmp = expr::compare(cmp_op, needle.clone(), expr)?;
                        Ok(cmp.into())
                    })
                    .collect::<Result<Vec<_>>>()?;

                // TODO: Error on no expressions?

                let conj = expr::conjunction(conj_op, cmp_exprs)?;
                Ok(conj.into())
            }
            ast::Expr::TypedString { datatype, value } => {
                let scalar = ScalarValue::Utf8(value.clone().into());
                // TODO: Add this back. Currently doing this to avoid having to
                // update cast rules for arrays and scalars at the same time.
                //
                // let scalar = cast_scalar(scalar, &datatype)?;
                Ok(Expression::Cast(CastExpr {
                    to: datatype.clone(),
                    expr: Box::new(Expression::Literal(LiteralExpr { literal: scalar })),
                }))
            }
            ast::Expr::Cast { datatype, expr } => {
                let expr = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;
                Ok(Expression::Cast(CastExpr {
                    to: datatype.clone(),
                    expr: Box::new(expr),
                }))
            }
            ast::Expr::Like {
                expr,
                pattern,
                negated,
                case_insensitive,
            } => {
                if *case_insensitive {
                    not_implemented!("case insensitive LIKE")
                }

                let expr = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;
                let pattern = self.bind_expression(
                    bind_context,
                    pattern,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let function = expr::bind_scalar_function(&FUNCTION_SET_LIKE, vec![expr, pattern])?;

                let mut expr = Expression::ScalarFunction(ScalarFunctionExpr { function });

                if *negated {
                    expr = expr::negate(NegateOperator::Not, expr)?.into();
                }

                Ok(expr)
            }
            ast::Expr::IsNull { expr, negated } => {
                let expr = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let function_set = if !negated {
                    &FUNCTION_SET_IS_NULL
                } else {
                    &FUNCTION_SET_IS_NOT_NULL
                };

                let function = expr::bind_scalar_function(function_set, vec![expr])?;

                Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
            }
            ast::Expr::IsBool { expr, val, negated } => {
                let expr = self.bind_expression(
                    bind_context,
                    expr,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                let function_set = match (val, negated) {
                    (true, false) => &FUNCTION_SET_IS_TRUE,
                    (true, true) => &FUNCTION_SET_IS_NOT_TRUE,
                    (false, false) => &FUNCTION_SET_IS_FALSE,
                    (false, true) => &FUNCTION_SET_IS_NOT_FALSE,
                };

                let function = expr::bind_scalar_function(function_set, vec![expr])?;

                Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
            }
            ast::Expr::Interval(ast::Interval {
                value,
                leading,
                trailing,
            }) => {
                if leading.is_some() {
                    return Err(RayexecError::new(
                        "Leading unit in interval not yet supported",
                    ));
                }
                let expr = self.bind_expression(
                    bind_context,
                    value,
                    column_binder,
                    RecursionContext {
                        is_root: false,
                        ..recur
                    },
                )?;

                match trailing {
                    Some(trailing) => {
                        // If a user provides a unit like `INTERVAL 3 YEARS`, we
                        // go ahead an multiply 3 with the a constant interval
                        // representing 1 YEAR.
                        //
                        // This builds on top of our existing casting/function
                        // dispatch rules. It's assumed that we have a
                        // `mul(interval, int64)` function (and similar).

                        let const_interval = match trailing {
                            ast::IntervalUnit::Year => Interval::new(12, 0, 0),
                            ast::IntervalUnit::Month => Interval::new(1, 0, 0),
                            ast::IntervalUnit::Week => Interval::new(0, 7, 0),
                            ast::IntervalUnit::Day => Interval::new(0, 1, 0),
                            ast::IntervalUnit::Hour => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_HOUR)
                            }
                            ast::IntervalUnit::Minute => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_MINUTE)
                            }
                            ast::IntervalUnit::Second => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_SECOND)
                            }
                            ast::IntervalUnit::Millisecond => {
                                Interval::new(0, 0, Interval::NANOSECONDS_IN_MILLISECOND)
                            }
                            other => {
                                // TODO: Got lazy, add the rest.
                                return Err(RayexecError::new(format!(
                                    "Missing interval constant for {other:?}"
                                )));
                            }
                        };

                        let interval = Expression::Literal(LiteralExpr {
                            literal: BorrowedScalarValue::Interval(const_interval),
                        });

                        let op = ArithOperator::Mul;
                        // Plan `mul(<interval>, <expr>)`
                        let expr = expr::arith(op, interval, expr)?;
                        Ok(expr.into())
                    }
                    None => Ok(Expression::Cast(CastExpr {
                        to: DataType::Interval,
                        expr: Box::new(expr),
                    })),
                }
            }
            ast::Expr::Between {
                negated,
                expr,
                low,
                high,
            } => {
                let bind = &mut |expr| {
                    self.bind_expression(
                        bind_context,
                        expr,
                        column_binder,
                        RecursionContext {
                            is_root: false,
                            ..recur
                        },
                    )
                };

                let expr = bind(expr)?;
                let low = bind(low)?;
                let high = bind(high)?;

                // c1 BETWEEN a AND b
                // c1 >= a AND c1 <= b
                //
                // c1 NOT BETWEEN a AND b
                // c1 < a OR c1 > b

                let low_op = if !negated {
                    ComparisonOperator::GtEq
                } else {
                    ComparisonOperator::Lt
                };

                let left = expr::compare(low_op, expr.clone(), low)?;

                let high_op = if !negated {
                    ComparisonOperator::LtEq
                } else {
                    ComparisonOperator::Gt
                };

                let right = expr::compare(low_op, expr, high)?;

                let conj_op = if !negated {
                    ConjunctionOperator::And
                } else {
                    ConjunctionOperator::Or
                };

                let conj = expr::conjunction(conj_op, [left.into(), right.into()])?;
                Ok(conj.into())
            }
            ast::Expr::Case {
                expr,
                conditions,
                results,
                else_expr,
            } => {
                if conditions.len() != results.len() {
                    return Err(RayexecError::new(
                        "CASE conditions and results differ in lengths",
                    ));
                }
                // Parser shouldn't allow this, but just in case.
                if conditions.is_empty() {
                    return Err(RayexecError::new("CASE requires at least one condition"));
                }

                let expr = expr
                    .as_ref()
                    .map(|expr| {
                        self.bind_expression(bind_context, expr, column_binder, recur.not_root())
                    })
                    .transpose()?;

                let mut else_expr = else_expr
                    .as_ref()
                    .map(|expr| {
                        self.bind_expression(bind_context, expr, column_binder, recur.not_root())
                    })
                    .transpose()?;

                let conditions = self.bind_expressions(
                    bind_context,
                    conditions,
                    column_binder,
                    recur.not_root(),
                )?;

                let results =
                    self.bind_expressions(bind_context, results, column_binder, recur.not_root())?;

                // When leading expr is provided, conditions are implicit equalities.
                let build_condition = |cond_expr| -> Result<Expression> {
                    match &expr {
                        Some(expr) => {
                            let cmp =
                                expr::compare(ComparisonOperator::Eq, expr.clone(), cond_expr)?;
                            Ok(cmp.into())
                        }
                        None => Ok(cond_expr),
                    }
                };

                // TODO: Cast the results so they all produce the same type.
                let mut cases = Vec::with_capacity(conditions.len());

                for (condition, result) in conditions.into_iter().zip(results) {
                    let condition = build_condition(condition)?;
                    cases.push(WhenThen {
                        when: condition,
                        then: result,
                    });
                }

                // Apply cast to else if needed.
                if let Some(expr) = else_expr {
                    let first_case_dt =
                        cases.first().expect("at least one case").then.datatype()?;

                    if expr.datatype()? != first_case_dt {
                        else_expr = Some(Expression::Cast(CastExpr {
                            to: first_case_dt,
                            expr: Box::new(expr),
                        }));
                    } else {
                        else_expr = Some(expr);
                    }
                }

                let case_expr = CaseExpr::try_new(cases, else_expr.map(Box::new))?;

                Ok(Expression::Case(case_expr))
            }
            ast::Expr::Substring { expr, from, count } => {
                let function_set = &FUNCTION_SET_SUBSTRING;
                let expr =
                    self.bind_expression(bind_context, expr, column_binder, recur.not_root())?;
                let from =
                    self.bind_expression(bind_context, from, column_binder, recur.not_root())?;

                let function = match count {
                    Some(count) => {
                        let count = self.bind_expression(
                            bind_context,
                            count,
                            column_binder,
                            recur.not_root(),
                        )?;

                        expr::bind_scalar_function(function_set, vec![expr, from, count])?
                    }
                    None => expr::bind_scalar_function(function_set, vec![expr, from])?,
                };

                Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
            }
            ast::Expr::Extract { date_part, expr } => {
                let date_part_expr = Expression::Literal(LiteralExpr {
                    literal: date_part.into_kw().to_string().into(),
                });

                let expr =
                    self.bind_expression(bind_context, expr, column_binder, recur.not_root())?;

                let function = expr::bind_scalar_function(
                    &FUNCTION_SET_DATE_PART,
                    vec![date_part_expr, expr],
                )?;

                Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
            }
            ast::Expr::Columns(_) => {
                // TODO: This doens't need to be the case, but there's going to
                // be slightly different handling if this is a top-level select
                // expression, and argument to a function, or used elsewhere in
                // the query.
                //
                // Currently we're just going to support top-level select
                // expressions.
                Err(RayexecError::new(
                    "COLUMNS expression should have been handle in select list expander",
                ))
            }
        }
    }

    pub(crate) fn bind_subquery(
        &self,
        bind_context: &mut BindContext,
        subquery: &QueryNode<ResolvedMeta>,
        subquery_type: SubqueryType,
    ) -> Result<Expression> {
        let nested = bind_context.new_child_scope(self.current);
        let bound =
            QueryBinder::new(nested, self.resolve_context).bind(bind_context, subquery.clone())?;

        let table = bind_context.get_table(bound.output_table_ref())?;
        let query_return_type = table
            .column_types
            .first()
            .cloned()
            .ok_or_else(|| RayexecError::new("Subquery returns zero columns"))?;

        let return_type = if subquery_type == SubqueryType::Scalar {
            query_return_type.clone()
        } else {
            DataType::Boolean
        };

        if matches!(
            subquery_type,
            SubqueryType::Scalar | SubqueryType::Any { .. }
        ) && table.num_columns() != 1
        {
            return Err(RayexecError::new(format!(
                "Expected subquery to return 1 column, returns {} columns",
                table.num_columns(),
            )));
        }

        // Apply cast to expression to try to match the output of the subquery
        // if needed.
        let subquery_type = match subquery_type {
            SubqueryType::Any { expr, op } => {
                if expr.datatype()? != return_type {
                    SubqueryType::Any {
                        expr: Box::new(Expression::Cast(CastExpr {
                            to: query_return_type,
                            expr,
                        })),
                        op,
                    }
                } else {
                    SubqueryType::Any { expr, op }
                }
            }
            other => other,
        };

        // Move correlated columns that don't reference the current scope to the
        // current scope's list of correlated columns.
        let mut current_correlations = Vec::new();
        for col in bind_context.correlated_columns(nested)? {
            if col.outer != self.current {
                current_correlations.push(col.clone());
            }
        }
        bind_context.push_correlations(self.current, current_correlations)?;

        Ok(Expression::Subquery(SubqueryExpr {
            bind_idx: nested,
            subquery: Box::new(bound),
            subquery_type,
            return_type,
        }))
    }

    pub(crate) fn bind_literal(literal: &ast::Literal<ResolvedMeta>) -> Result<Expression> {
        Ok(match literal {
            ast::Literal::Number(n) => {
                if let Ok(n) = n.parse::<i32>() {
                    Expression::Literal(LiteralExpr {
                        literal: ScalarValue::Int32(n),
                    })
                } else if let Ok(n) = n.parse::<i64>() {
                    Expression::Literal(LiteralExpr {
                        literal: ScalarValue::Int64(n),
                    })
                } else if let Ok(n) = n.parse::<u64>() {
                    Expression::Literal(LiteralExpr {
                        literal: ScalarValue::UInt64(n),
                    })
                } else if let Ok(n) = n.parse::<f64>() {
                    Expression::Literal(LiteralExpr {
                        literal: ScalarValue::Float64(n),
                    })
                } else {
                    return Err(RayexecError::new(format!(
                        "Unable to parse {n} as a number"
                    )));
                }
            }
            ast::Literal::Boolean(b) => Expression::Literal(LiteralExpr {
                literal: ScalarValue::Boolean(*b),
            }),
            ast::Literal::Null => Expression::Literal(LiteralExpr {
                literal: ScalarValue::Null,
            }),
            ast::Literal::SingleQuotedString(s) => Expression::Literal(LiteralExpr {
                literal: ScalarValue::Utf8(s.to_string().into()),
            }),
            other => {
                return Err(RayexecError::new(format!(
                    "Unusupported SQL literal: {other:?}"
                )))
            }
        })
    }

    pub(crate) fn bind_function(
        &self,
        bind_context: &mut BindContext,
        func: &ast::Function<ResolvedMeta>,
        column_binder: &mut impl ExpressionColumnBinder,
        recur: RecursionContext,
    ) -> Result<Expression> {
        let reference = self
            .resolve_context
            .functions
            .try_get_bound(func.reference)?;

        let recur = if reference.0.is_aggregate() {
            RecursionContext {
                allow_windows: false,
                allow_aggregates: false,
                ..recur
            }
        } else {
            recur
        };

        let inputs = func
            .args
            .iter()
            .map(|arg| match arg {
                ast::FunctionArg::Unnamed { arg } => match arg {
                    ast::FunctionArgExpr::Expr(expr) => Ok(self.bind_expression(
                        bind_context,
                        expr,
                        column_binder,
                        RecursionContext {
                            is_root: false,
                            ..recur
                        },
                    )?),
                    ast::FunctionArgExpr::Wildcard => {
                        // Resolver should have handled removing '*'
                        // from function calls.
                        Err(RayexecError::new(
                            "Cannot plan a function with '*' as an argument",
                        ))
                    }
                },
                ast::FunctionArg::Named { .. } => Err(RayexecError::new(
                    "Named arguments to scalar functions not supported",
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        // TODO: This should probably assert that location == any since
        // I don't think it makes sense to try to handle different sets
        // of scalar/aggs in the hybrid case yet.
        match reference {
            (ResolvedFunction::Special(special), _) => {
                match special {
                    SpecialBuiltinFunction::Unnest => {
                        if func.distinct || func.filter.is_some() || func.over.is_some() {
                            return Err(RayexecError::new(
                                "UNNEST does not support DISTINCT, FILTER, or OVER",
                            ));
                        }

                        if func.args.len() != 1 {
                            return Err(RayexecError::new("UNNEST requires a single argument"));
                        }

                        let input = match &func.args[0] {
                            ast::FunctionArg::Named { .. } => {
                                return Err(RayexecError::new(
                                    "named arguments to UNNEST not yet supported",
                                ))
                            }
                            ast::FunctionArg::Unnamed { arg } => match arg {
                                ast::FunctionArgExpr::Wildcard => {
                                    return Err(RayexecError::new(
                                        "wildcard to UNNEST not supported",
                                    ))
                                }
                                ast::FunctionArgExpr::Expr(expr) => self.bind_expression(
                                    bind_context,
                                    expr,
                                    column_binder,
                                    RecursionContext {
                                        is_root: false,
                                        ..recur
                                    },
                                )?,
                            },
                        };

                        let unnest_expr = Expression::Unnest(UnnestExpr {
                            expr: Box::new(input),
                        });

                        // To verify input types.
                        let _ = unnest_expr.datatype()?;

                        Ok(unnest_expr)
                    }
                    SpecialBuiltinFunction::Grouping => {
                        if func.distinct || func.filter.is_some() || func.over.is_some() {
                            return Err(RayexecError::new(
                                "GROUPING does not support DISTINCT, FILTER, or OVER",
                            ));
                        }

                        if func.args.is_empty() {
                            return Err(RayexecError::new(
                                "GROUPING requires at least one argument",
                            ));
                        }

                        let inputs = func
                            .args
                            .iter()
                            .map(|arg| match arg {
                                ast::FunctionArg::Named { .. } => Err(RayexecError::new(
                                    "GROUPING does not accept named arguments",
                                )),
                                ast::FunctionArg::Unnamed { arg } => match arg {
                                    ast::FunctionArgExpr::Wildcard => Err(RayexecError::new(
                                        "GROUPING does not support wildcard arguments",
                                    )),
                                    ast::FunctionArgExpr::Expr(expr) => self.bind_expression(
                                        bind_context,
                                        expr,
                                        column_binder,
                                        RecursionContext {
                                            is_root: false,
                                            ..recur
                                        },
                                    ),
                                },
                            })
                            .collect::<Result<Vec<_>>>()?;

                        Ok(Expression::GroupingSet(GroupingSetExpr { inputs }))
                    }
                }
            }
            (ResolvedFunction::Scalar(scalar), _) => {
                if func.distinct {
                    return Err(RayexecError::new(
                        "DISTINCT only supported for aggregate functions",
                    ));
                }
                if func.over.is_some() {
                    return Err(RayexecError::new(
                        "OVER only supported for aggregate functions",
                    ));
                }

                let function = expr::bind_scalar_function(&scalar, inputs)?;

                Ok(Expression::ScalarFunction(ScalarFunctionExpr { function }))
            }
            (ResolvedFunction::Aggregate(agg), _) => {
                let agg = bind_aggregate_function(&agg, inputs)?;
                match &func.over {
                    Some(over) => {
                        // Window

                        match over {
                            ast::WindowSpec::Named(_) => {
                                not_implemented!("named window spec")
                            }
                            ast::WindowSpec::Definition(window_def) => {
                                if window_def.existing.is_some() {
                                    not_implemented!("inherit existing window spec definition")
                                }

                                let partition_by = self.bind_expressions(
                                    bind_context,
                                    &window_def.partition_by,
                                    column_binder,
                                    recur,
                                )?;

                                // Handle order by.
                                //
                                // Handled slightly different than statement
                                // level ORDER BY in that it can't bind to an
                                // output column.
                                let order_by = window_def
                                    .order_by
                                    .iter()
                                    .map(|order_by| {
                                        let expr = self.bind_expression(
                                            bind_context,
                                            &order_by.expr,
                                            column_binder,
                                            recur,
                                        )?;
                                        Ok(BoundOrderByExpr {
                                            expr,
                                            desc: matches!(
                                                order_by.typ.unwrap_or(ast::OrderByType::Asc),
                                                ast::OrderByType::Desc
                                            ),
                                            nulls_first: matches!(
                                                order_by.nulls.unwrap_or(ast::OrderByNulls::First),
                                                ast::OrderByNulls::First
                                            ),
                                        })
                                    })
                                    .collect::<Result<Vec<_>>>()?;

                                let start = match &window_def.frame {
                                    Some(_frame) => {
                                        not_implemented!("non-default window frame")
                                    }
                                    None => WindowFrameBound::default_start(),
                                };

                                let end = match &window_def.frame {
                                    Some(_frame) => {
                                        not_implemented!("non-default window frame")
                                    }
                                    None => WindowFrameBound::default_start(),
                                };

                                let exclude = match &window_def.frame {
                                    Some(_frame) => {
                                        not_implemented!("non-default window frame")
                                    }
                                    None => WindowFrameExclusion::default(),
                                };

                                Ok(Expression::Window(WindowExpr {
                                    agg,
                                    partition_by,
                                    order_by,
                                    start,
                                    end,
                                    exclude,
                                }))
                            }
                        }
                    }
                    None => {
                        // Normal aggregate.
                        Ok(Expression::Aggregate(AggregateExpr {
                            agg,
                            distinct: func.distinct,
                            filter: None,
                        }))
                    }
                }
            }
        }
    }

    // TODO: Reduce dupliation with the scalar one.
    fn apply_casts_for_aggregate_function(
        &self,
        bind_context: &BindContext,
        agg: &dyn AggregateFunction2,
        inputs: Vec<Expression>,
    ) -> Result<Vec<Expression>> {
        let input_datatypes = inputs
            .iter()
            .map(|expr| expr.datatype())
            .collect::<Result<Vec<_>>>()?;

        if agg.exact_signature(&input_datatypes).is_some() {
            // Exact
            Ok(inputs)
        } else {
            // Try to find candidates that we can cast to.
            let mut candidates = agg.candidate(&input_datatypes);

            if candidates.is_empty() {
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    agg.name(),
                    input_datatypes.display_with_brackets(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            let inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => Expression::Cast(CastExpr {
                            to: DataType::try_default_datatype(to)?,
                            expr: Box::new(input),
                        }),
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(inputs)
        }
    }

    // TODO: Reduce duplication with scalar one.
    //
    // Upcasting to FunctionInfo would be cool.
    // See: <https://github.com/rust-lang/rust/issues/65991>
    pub(crate) fn apply_casts_for_table_function(
        &self,
        bind_context: &BindContext,
        table: &dyn TableFunction,
        inputs: Vec<Expression>,
    ) -> Result<Vec<Expression>> {
        let input_datatypes = inputs
            .iter()
            .map(|expr| expr.datatype())
            .collect::<Result<Vec<_>>>()?;

        if table.exact_signature(&input_datatypes).is_some() {
            // Exact
            Ok(inputs)
        } else {
            // Try to find candidates that we can cast to.
            let mut candidates = table.candidate(&input_datatypes);

            if candidates.is_empty() {
                // TODO: Better error.
                return Err(RayexecError::new(format!(
                    "Invalid inputs to '{}': {}",
                    table.name(),
                    input_datatypes.display_with_brackets(),
                )));
            }

            // TODO: Maybe more sophisticated candidate selection.
            let candidate = candidates.swap_remove(0);

            // Apply casts where needed.
            let inputs = inputs
                .into_iter()
                .zip(candidate.casts)
                .map(|(input, cast_to)| {
                    Ok(match cast_to {
                        CastType::Cast { to, .. } => Expression::Cast(CastExpr {
                            to: DataType::try_default_datatype(to)?,
                            expr: Box::new(input),
                        }),
                        CastType::NoCastNeeded => input,
                    })
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(inputs)
        }
    }
}
