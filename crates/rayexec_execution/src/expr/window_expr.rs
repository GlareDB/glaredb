use std::fmt;

use fmtutil::IntoDisplayableSlice;
use rayexec_error::Result;

use super::Expression;
use crate::arrays::datatype::DataType;
use crate::explain::context_display::{ContextDisplay, ContextDisplayMode, ContextDisplayWrapper};
use crate::functions::aggregate::PlannedAggregateFunction;
use crate::logical::binder::bind_query::bind_modifier::BoundOrderByExpr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowFrameUnit {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WindowFrameExclusion {
    /// Exclude the current row.
    ExcludeCurrentRow,
    /// Exclude the current row and ordering peers.
    ExcludeGroup,
    /// Exclude peers of current row, but not the current row itself.
    ExcludeTies,
    /// Don't exlude current row or peers.
    #[default]
    ExcludeNoOthers,
}

/// The window frame bound.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WindowFrameBound {
    UnboundedPreceding(WindowFrameUnit),
    Preceding(WindowFrameUnit, i64),
    UnboundedFollowing(WindowFrameUnit),
    Following(WindowFrameUnit, i64),
    CurrentRow(WindowFrameUnit),
}

impl WindowFrameBound {
    /// The default start bound for a window function.
    ///
    /// The default start/end bounds is:
    /// `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    ///
    /// Default unit is RANGE.
    pub const fn default_start() -> Self {
        WindowFrameBound::UnboundedPreceding(WindowFrameUnit::Range)
    }

    /// The default end bound for a window function.
    ///
    /// See `default_start` for the complete bounds.
    pub const fn default_end() -> Self {
        WindowFrameBound::CurrentRow(WindowFrameUnit::Range)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowExpr {
    /// The aggregate function.
    // TODO: May need to adjust to allow for window-only functions.
    pub agg: PlannedAggregateFunction,
    /// How to partition the input to the function.
    pub partition_by: Vec<Expression>,
    /// How the input is ordered within a partition.
    pub order_by: Vec<BoundOrderByExpr>,
    /// Start bound for the window.
    pub start: WindowFrameBound,
    /// End bound for the window.
    pub end: WindowFrameBound,
    /// Rows to exclude in the window.
    pub exclude: WindowFrameExclusion,
}

impl WindowExpr {
    pub fn datatype(&self) -> Result<DataType> {
        Ok(self.agg.state.return_type.clone())
    }
}

impl ContextDisplay for WindowExpr {
    fn fmt_using_context(
        &self,
        mode: ContextDisplayMode,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        write!(f, "{}", self.agg.name)?;
        let inputs = self
            .agg
            .state
            .inputs
            .iter()
            .map(|expr| ContextDisplayWrapper::with_mode(expr, mode).to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "({}) OVER (", inputs)?;

        if !self.partition_by.is_empty() {
            write!(
                f,
                "PARTION BY {} ",
                self.partition_by
                    .iter()
                    .map(|expr| ContextDisplayWrapper::with_mode(expr, mode))
                    .collect::<Vec<_>>()
                    .display_as_list()
            )?;
        }

        if !self.order_by.is_empty() {
            write!(f, "ORDER BY {} ", self.order_by.display_as_list())?;
        }

        // TODO: Bounds

        write!(f, ")")?;

        Ok(())
    }
}
