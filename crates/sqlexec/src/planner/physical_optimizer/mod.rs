use std::sync::Arc;

use datafusion::{
    common::tree_node::{RewriteRecursion, TreeNodeRewriter},
    error::Result,
    physical_plan::{aggregates::AggregateExec, ExecutionPlan},
};

#[derive(Debug, Default)]
pub struct MetadataOnlyOptimizer;
