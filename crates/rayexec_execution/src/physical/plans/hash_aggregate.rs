use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};

/// Aggregates with grouping.
pub struct PhysicalHashAggregate {}

impl Explainable for PhysicalHashAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashAggregate")
    }
}
