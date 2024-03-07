use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};

pub struct PhysicalOrder {}

impl Explainable for PhysicalOrder {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Order")
    }
}
