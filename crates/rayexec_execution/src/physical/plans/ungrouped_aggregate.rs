
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};










/// Aggregates with no grouping.
pub struct PhysicalUngroupedAggregate {}

impl Explainable for PhysicalUngroupedAggregate {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("UngroupedAggregate")
    }
}
