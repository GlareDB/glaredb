use crate::arrow::column::Column;
use crate::arrow::expr::ScalarExpr;
use crate::arrow::scalar::ScalarOwned;
use crate::errors::{internal, LemurError, Result};
use tracing::trace;

use super::{Accumulator, AggregateExpr};

/// A count aggregate expression.
#[derive(Debug)]
pub struct Count {
    expr: ScalarExpr,
}

impl Count {
    pub fn new(expr: ScalarExpr) -> Count {
        Count { expr }
    }

    /// Produce a count aggregate equivalent to "COUNT(*)"
    pub fn count_star() -> Count {
        Count {
            expr: ScalarExpr::Constant(ScalarOwned::Bool(Some(true))),
        }
    }
}

impl AggregateExpr for Count {
    fn inputs(&self) -> Vec<ScalarExpr> {
        vec![self.expr.clone()]
    }

    fn accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(CountAccumulator { current: 0 })
    }
}

#[derive(Debug)]
struct CountAccumulator {
    current: i64,
}

impl Accumulator for CountAccumulator {
    fn accumulate(&mut self, cols: &[Column]) -> Result<()> {
        trace!("acc");
        let col = cols
            .get(0)
            .ok_or_else(|| internal!("missing column for accumulation"))?;

        let count: i64 = col.non_null_count().try_into()?;
        match count.checked_add(self.current) {
            Some(total) => self.current = total,
            None => return Err(LemurError::Overflow),
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarOwned> {
        Ok(ScalarOwned::Int64(Some(self.current)))
    }
}
