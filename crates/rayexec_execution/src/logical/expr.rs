use super::operator::LogicalOperator;
use super::sql::scope::ColumnRef;
use crate::expr::scalar::VariadicOperator;
use crate::expr::scalar::{PlannedBinaryOperator, PlannedUnaryOperator};
use crate::functions::aggregate::PlannedAggregateFunction;
use crate::functions::scalar::PlannedScalarFunction;
use rayexec_bullet::datatype::DataType;
use rayexec_bullet::field::TypeSchema;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{RayexecError, Result};
use std::fmt;

/// Types of subqueries that can exist in expressions.
#[derive(Debug, Clone, PartialEq)]
pub enum Subquery {
    /// Scalar subquery
    Scalar { root: Box<LogicalOperator> },
    /// An EXISTS subquery, possibly negated.
    Exists {
        root: Box<LogicalOperator>,
        negated: bool,
    },
    /// An ANY or IN subquery.
    Any { root: Box<LogicalOperator> },
}

impl Subquery {
    /// Takes the operator root for the subquery, replacing it with an empty
    /// operator.
    pub fn take_root(&mut self) -> Box<LogicalOperator> {
        match self {
            Self::Scalar { root } => std::mem::replace(root, Box::new(LogicalOperator::Empty)),
            Self::Exists { root, .. } => std::mem::replace(root, Box::new(LogicalOperator::Empty)),
            Self::Any { root } => std::mem::replace(root, Box::new(LogicalOperator::Empty)),
        }
    }

    pub fn get_root_mut(&mut self) -> &mut LogicalOperator {
        match self {
            Self::Scalar { root } => root,
            Self::Exists { root, .. } => root,
            Self::Any { root } => root,
        }
    }

    pub fn is_correlated(&mut self) -> Result<bool> {
        let mut correlated = false;
        self.get_root_mut().walk_mut_pre(&mut |plan| {
            match plan {
                LogicalOperator::Projection(node) => {
                    node.as_mut()
                        .exprs
                        .iter_mut()
                        .for_each(|expr| correlated |= expr.is_correlated());
                }
                LogicalOperator::Filter(node) => {
                    correlated = node.as_mut().predicate.is_correlated();
                }
                _ => (), // TODO: The others
            }
            Ok(())
        })?;

        Ok(correlated)
    }
}

/// An expression that can exist in a logical plan.
#[derive(Debug, Clone, PartialEq)]
pub enum LogicalExpression {
    /// Reference to a column.
    ///
    /// Note that this includes scoping information since this expression can be
    /// part of a correlated subquery.
    ColumnRef(ColumnRef),

    /// Literal value.
    Literal(OwnedScalarValue),

    /// A function that returns a single value.
    ScalarFunction {
        function: Box<dyn PlannedScalarFunction>,
        inputs: Vec<LogicalExpression>,
    },

    /// Cast an expression to some type.
    Cast {
        to: DataType,
        expr: Box<LogicalExpression>,
    },

    /// Unary operator.
    Unary {
        op: PlannedUnaryOperator,
        expr: Box<LogicalExpression>,
    },

    /// Binary operator.
    Binary {
        op: PlannedBinaryOperator,
        left: Box<LogicalExpression>,
        right: Box<LogicalExpression>,
    },

    /// Variadic operator.
    Variadic {
        op: VariadicOperator,
        exprs: Vec<LogicalExpression>,
    },

    /// An aggregate function.
    Aggregate {
        /// The function.
        agg: Box<dyn PlannedAggregateFunction>,

        /// Input expressions to the aggragate.
        inputs: Vec<LogicalExpression>,

        /// Optional filter to the aggregate.
        filter: Option<Box<LogicalExpression>>,
    },

    /// A subquery.
    Subquery(Subquery),

    /// Case expressions.
    Case {
        input: Box<LogicalExpression>,
        /// When <left>, then <right>
        when_then: Vec<(LogicalExpression, LogicalExpression)>,
    },
}

impl fmt::Display for LogicalExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ColumnRef(col) => {
                if col.scope_level == 0 {
                    write!(f, "#{}", col.item_idx)
                } else {
                    write!(f, "{}#{}", col.scope_level, col.item_idx)
                }
            }
            Self::ScalarFunction {
                function, inputs, ..
            } => write!(
                f,
                "{}({})",
                function.scalar_function().name(),
                inputs
                    .iter()
                    .map(|input| input.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Self::Cast { to, expr } => write!(f, "CAST({expr}, {to})"),
            Self::Literal(val) => write!(f, "{val}"),
            Self::Unary { op, expr } => write!(f, "{}{expr}", op.op),
            Self::Binary { op, left, right } => write!(f, "{left}{}{right}", op.op),
            Self::Variadic { .. } => write!(f, "VARIADIC TODO"),
            Self::Aggregate {
                agg,
                inputs,
                filter,
            } => {
                write!(
                    f,
                    "{}({})",
                    agg.aggregate_function().name(),
                    inputs
                        .iter()
                        .map(|input| input.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                )?;
                if let Some(filter) = filter {
                    write!(f, " FILTER ({filter})")?;
                }
                Ok(())
            }
            Self::Subquery(_) => write!(f, "SUBQUERY TODO"),
            Self::Case { .. } => write!(f, "CASE TODO"),
        }
    }
}

impl LogicalExpression {
    /// Create a new uncorrelated column reference.
    pub fn new_column(col: usize) -> Self {
        LogicalExpression::ColumnRef(ColumnRef {
            scope_level: 0,
            item_idx: col,
        })
    }

    /// Get the output data type of this expression.
    ///
    /// Since we're working with possibly correlated columns, both the schema of
    /// the scope and the schema of the outer scopes are provided.
    pub fn datatype(&self, current: &TypeSchema, outer: &[TypeSchema]) -> Result<DataType> {
        Ok(match self {
            LogicalExpression::ColumnRef(col) => {
                if col.scope_level == 0 {
                    // Get data type from current schema.
                    current.types.get(col.item_idx).cloned().ok_or_else(|| {
                        RayexecError::new(format!(
                            "Column reference '{}' points to outside of current schema: {current:?}",
                            col.item_idx
                        ))
                    })?
                } else {
                    // Get data type from one of the outer schemas.
                    outer
                        .get(col.scope_level - 1)
                        .ok_or_else(|| {
                            RayexecError::new("Column reference points to non-existent schema")
                        })?
                        .types
                        .get(col.item_idx)
                        .cloned()
                        .ok_or_else(|| {
                            RayexecError::new("Column reference points to outside of outer schema")
                        })?
                }
            }
            LogicalExpression::Literal(lit) => lit.datatype(),
            LogicalExpression::ScalarFunction { function, .. } => function.return_type(),
            LogicalExpression::Cast { to, .. } => to.clone(),
            LogicalExpression::Aggregate { agg, .. } => agg.return_type(),
            LogicalExpression::Unary { op, .. } => op.scalar.return_type(),
            LogicalExpression::Binary { op, .. } => op.scalar.return_type(),
            LogicalExpression::Subquery(subquery) => match subquery {
                Subquery::Scalar { root } => {
                    // TODO: Do we just need outer, or do we want current + outer?
                    let mut schema = root.output_schema(outer)?;
                    match schema.types.len() {
                        1 => schema.types.pop().unwrap(),
                        other => {
                            return Err(RayexecError::new(format!(
                                "Scalar subqueries should return 1 value, got {other}",
                            )))
                        }
                    }
                }
                Subquery::Any { .. } | Subquery::Exists { .. } => DataType::Boolean,
            },
            _ => unimplemented!(),
        })
    }

    /// Checks if this expression has a subquery component.
    pub fn is_subquery(&self) -> bool {
        matches!(self, Self::Subquery(_))
    }

    /// Checks if this expressions contains a reference to an outer scope.
    pub fn is_correlated(&self) -> bool {
        // Note the slight differen from `is_constant`. An expression is
        // correlated if _any_ of the expressions are correlated.
        match self {
            Self::ColumnRef(col) => col.scope_level > 0,
            Self::Literal(_) => false,
            Self::ScalarFunction { inputs, .. } => inputs.iter().any(|expr| expr.is_correlated()),
            Self::Cast { expr, .. } => expr.is_correlated(),
            Self::Unary { expr, .. } => expr.is_correlated(),
            Self::Binary { left, right, .. } => left.is_correlated() || right.is_correlated(),
            Self::Variadic { exprs, .. } => exprs.iter().any(|expr| expr.is_correlated()),
            Self::Aggregate { inputs, .. } => inputs.iter().any(|expr| expr.is_correlated()),
            Self::Subquery(_) => false,
            Self::Case { .. } => false,
        }
    }

    /// Checks if this is a constant expression.
    pub fn is_constant(&self) -> bool {
        match self {
            Self::ColumnRef(_) => false,
            Self::Literal(_) => true,
            Self::ScalarFunction { inputs, .. } => inputs.iter().all(|expr| expr.is_constant()),
            Self::Cast { expr, .. } => expr.is_constant(),
            Self::Unary { expr, .. } => expr.is_constant(),
            Self::Binary { left, right, .. } => left.is_constant() && right.is_constant(),
            Self::Variadic { exprs, .. } => exprs.iter().all(|expr| expr.is_constant()),
            Self::Aggregate { inputs, .. } => inputs.iter().all(|expr| expr.is_constant()),
            Self::Subquery(_) => false,
            Self::Case { .. } => false,
        }
    }

    pub fn walk_mut_pre<F>(&mut self, pre: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        self.walk_mut(pre, &mut |_| Ok(()))
    }

    pub fn walk_mut_post<F>(&mut self, post: &mut F) -> Result<()>
    where
        F: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        self.walk_mut(&mut |_| Ok(()), post)
    }

    /// Walk the expression depth first.
    ///
    /// `pre` provides access to children on the way down, and `post` on the way
    /// up.
    pub fn walk_mut<F1, F2>(&mut self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(&mut LogicalExpression) -> Result<()>,
        F2: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        /// Helper function for walking a subquery in an expression.
        fn walk_subquery<F1, F2>(
            plan: &mut LogicalOperator,
            pre: &mut F1,
            post: &mut F2,
        ) -> Result<()>
        where
            F1: FnMut(&mut LogicalExpression) -> Result<()>,
            F2: FnMut(&mut LogicalExpression) -> Result<()>,
        {
            match plan {
                LogicalOperator::Projection(p) => {
                    LogicalExpression::walk_mut_many(&mut p.as_mut().exprs, pre, post)?
                }
                LogicalOperator::Filter(p) => p.as_mut().predicate.walk_mut(pre, post)?,
                LogicalOperator::Aggregate(p) => {
                    LogicalExpression::walk_mut_many(&mut p.as_mut().aggregates, pre, post)?;
                    LogicalExpression::walk_mut_many(&mut p.as_mut().group_exprs, pre, post)?;
                }
                LogicalOperator::Order(p) => {
                    let p = p.as_mut();
                    for expr in &mut p.exprs {
                        expr.expr.walk_mut(pre, post)?;
                    }
                }
                LogicalOperator::AnyJoin(p) => p.as_mut().on.walk_mut(pre, post)?,
                LogicalOperator::EqualityJoin(_) => (),
                LogicalOperator::CrossJoin(_) => (),
                LogicalOperator::DependentJoin(_) => (),
                LogicalOperator::SetOperation(_) => (),
                LogicalOperator::Limit(_) => (),
                LogicalOperator::MaterializedScan(_) => (),
                LogicalOperator::Scan(_) => (),
                LogicalOperator::TableFunction(_) => (),
                LogicalOperator::ExpressionList(p) => {
                    let p = p.as_mut();
                    for row in &mut p.rows {
                        LogicalExpression::walk_mut_many(row, pre, post)?;
                    }
                }
                LogicalOperator::SetVar(_) => (),
                LogicalOperator::ShowVar(_) => (),
                LogicalOperator::ResetVar(_) => (),
                LogicalOperator::Insert(_) => (),
                LogicalOperator::CopyTo(_) => (),
                LogicalOperator::CreateSchema(_) => (),
                LogicalOperator::CreateTable(_) => (),
                LogicalOperator::CreateTableAs(_) => (),
                LogicalOperator::AttachDatabase(_) => (),
                LogicalOperator::DetachDatabase(_) => (),
                LogicalOperator::Explain(_) => (),
                LogicalOperator::Drop(_) => (),
                LogicalOperator::Empty => (),
                LogicalOperator::Describe(_) => (),
            }
            Ok(())
        }

        pre(self)?;
        match self {
            LogicalExpression::Unary { expr, .. } => {
                pre(expr)?;
                expr.walk_mut(pre, post)?;
                post(expr)?;
            }
            LogicalExpression::Cast { expr, .. } => {
                pre(expr)?;
                expr.walk_mut(pre, post)?;
                post(expr)?;
            }
            Self::Binary { left, right, .. } => {
                pre(left)?;
                left.walk_mut(pre, post)?;
                post(left)?;

                pre(right)?;
                right.walk_mut(pre, post)?;
                post(right)?;
            }
            Self::Variadic { exprs, .. } => {
                for expr in exprs.iter_mut() {
                    pre(expr)?;
                    expr.walk_mut(pre, post)?;
                    post(expr)?;
                }
            }
            Self::ScalarFunction { inputs, .. } => {
                for input in inputs.iter_mut() {
                    pre(input)?;
                    input.walk_mut(pre, post)?;
                    post(input)?;
                }
            }
            Self::Aggregate { inputs, .. } => {
                for input in inputs.iter_mut() {
                    pre(input)?;
                    input.walk_mut(pre, post)?;
                    post(input)?;
                }
            }
            Self::ColumnRef(_) | Self::Literal(_) => (),
            Self::Subquery(subquery) => {
                // We only care about the expressions in the plan, so it's
                // sufficient to walk the operators only once on the way down.
                subquery
                    .get_root_mut()
                    .walk_mut_pre(&mut |plan| walk_subquery(plan, pre, post))?;
            }
            Self::Case { .. } => unimplemented!(),
        }
        post(self)?;

        Ok(())
    }

    pub fn walk_mut_many<'a, F1, F2>(
        exprs: impl IntoIterator<Item = &'a mut Self>,
        pre: &mut F1,
        post: &mut F2,
    ) -> Result<()>
    where
        F1: FnMut(&mut LogicalExpression) -> Result<()>,
        F2: FnMut(&mut LogicalExpression) -> Result<()>,
    {
        for expr in exprs {
            expr.walk_mut(pre, post)?;
        }
        Ok(())
    }

    /// Check if this expression is an aggregate.
    pub const fn is_aggregate(&self) -> bool {
        matches!(self, LogicalExpression::Aggregate { .. })
    }

    /// Try to get a top-level literal from this expression, erroring if it's
    /// not one.
    pub fn try_into_scalar(self) -> Result<OwnedScalarValue> {
        match self {
            Self::Literal(lit) => Ok(lit),
            other => Err(RayexecError::new(format!("Not a literal: {other:?}"))),
        }
    }

    pub fn try_into_column_ref(self) -> Result<ColumnRef> {
        match self {
            Self::ColumnRef(col) => Ok(col),
            other => Err(RayexecError::new(format!(
                "Not a column reference: {other:?}"
            ))),
        }
    }
}

impl AsMut<LogicalExpression> for LogicalExpression {
    fn as_mut(&mut self) -> &mut LogicalExpression {
        self
    }
}
