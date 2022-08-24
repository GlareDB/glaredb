use anyhow::{anyhow, Result};
use lemur::repr::expr::{AggregateOperation, BinaryOperation, ScalarExpr, UnaryOperation};
use lemur::repr::value::Value;
use tracing::trace;

/// An intermediate expression representation that's used during planning.
///
/// This is used during planning to allow for intermixing of aggregates and
/// window functions in select items. When we encounter those, we're able to
/// throw them into the expression tree as is, and then come back later to
/// rewrite them.
///
/// This may only be lowered into a scalar expression if there are no aggregates
/// or window functions in the tree.
#[derive(Debug, Clone)]
pub enum PlanExpr {
    Column(usize),
    Constant(Value),
    Unary {
        op: UnaryOperation,
        input: Box<PlanExpr>,
    },
    Binary {
        op: BinaryOperation,
        left: Box<PlanExpr>,
        right: Box<PlanExpr>,
    },
    Aggregate {
        op: AggregateOperation,
        arg: Box<PlanExpr>,
    },
}

impl PlanExpr {
    /// Whether or not this is a scalar expression.
    pub fn is_scalar(&self) -> bool {
        match self {
            PlanExpr::Column(_) => true,
            PlanExpr::Constant(_) => true,
            PlanExpr::Unary { input, .. } => input.is_scalar(),
            PlanExpr::Binary { left, right, .. } => left.is_scalar() && right.is_scalar(),
            PlanExpr::Aggregate { .. } => false,
        }
    }

    pub fn boxed(self) -> Box<PlanExpr> {
        Box::new(self)
    }

    pub fn try_get_column(&self) -> Option<usize> {
        match self {
            PlanExpr::Column(idx) => Some(*idx),
            _ => None,
        }
    }

    pub fn lower_scalar(self) -> Result<ScalarExpr> {
        trace!(?self, "lowering scalar expression");
        Ok(match self {
            PlanExpr::Column(idx) => ScalarExpr::Column(idx),
            PlanExpr::Constant(val) => ScalarExpr::Constant(val),
            PlanExpr::Unary { op, input } => ScalarExpr::Unary {
                op,
                input: input.lower_scalar()?.boxed(),
            },
            PlanExpr::Binary { op, left, right } => ScalarExpr::Binary {
                op,
                left: left.lower_scalar()?.boxed(),
                right: right.lower_scalar()?.boxed(),
            },
            PlanExpr::Aggregate { .. } => {
                return Err(anyhow!("encountered aggregate when lowering to scalar"))
            }
        })
    }

    fn replace<F>(&mut self, mut f: F) -> Result<()>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let temp = std::mem::replace(self, PlanExpr::Constant(Value::Null));
        *self = f(temp)?;
        Ok(())
    }

    pub fn walk<F1, F2>(&self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(&Self) -> Result<()>,
        F2: FnMut(&Self) -> Result<()>,
    {
        pre(self)?;
        match self {
            PlanExpr::Column(_) | PlanExpr::Constant(_) => (),
            PlanExpr::Unary { input, .. } => {
                input.walk(pre, post)?;
            }
            PlanExpr::Binary { left, right, .. } => {
                left.walk(pre, post)?;
                right.walk(pre, post)?;
            }
            PlanExpr::Aggregate { arg, .. } => arg.walk(pre, post)?,
        }
        post(self)?;
        Ok(())
    }

    pub fn transform<F1, F2>(mut self, pre: &mut F1, post: &mut F2) -> Result<Self>
    where
        F1: FnMut(Self) -> Result<Self>,
        F2: FnMut(Self) -> Result<Self>,
    {
        self = pre(self)?;
        match &mut self {
            PlanExpr::Column(_) | PlanExpr::Constant(_) => (),
            PlanExpr::Unary { input, .. } => {
                input.replace(|expr| expr.transform(pre, post))?;
            }
            PlanExpr::Binary { left, right, .. } => {
                left.replace(|expr| expr.transform(pre, post))?;
                right.replace(|expr| expr.transform(pre, post))?;
            }
            PlanExpr::Aggregate { arg, .. } => arg.replace(|expr| expr.transform(pre, post))?,
        }
        self = post(self)?;
        Ok(self)
    }

    pub fn transform_mut<F1, F2>(&mut self, pre: &mut F1, post: &mut F2) -> Result<()>
    where
        F1: FnMut(Self) -> Result<Self>,
        F2: FnMut(Self) -> Result<Self>,
    {
        self.replace(|expr| expr.transform(pre, post))
    }

    pub fn transform_mut_pre<F>(&mut self, pre: &mut F) -> Result<()>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        self.replace(|expr| expr.transform(pre, &mut Ok))
    }
}
