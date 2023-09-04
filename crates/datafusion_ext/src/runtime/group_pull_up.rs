use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, NestedLoopJoinExec, SortMergeJoinExec};
use datafusion::physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::{InterleaveExec, UnionExec};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

use crate::runtime::runtime_group::RuntimeGroupExec;

/// Tries to pull up `RuntimeGroupExec`s as far as possible.
#[derive(Debug, Default, Clone, Copy)]
pub struct RuntimeGroupPullUp {}

impl RuntimeGroupPullUp {
    pub fn new() -> Self {
        RuntimeGroupPullUp {}
    }
}

impl PhysicalOptimizerRule for RuntimeGroupPullUp {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if !can_pull_through_node(plan.as_ref()) {
                return Ok(Transformed::No(plan));
            }

            // Get all children for this plan. If all children are runtime group
            // execs, we can potentially pull it up.
            let mut children: Vec<RuntimeGroupExec> = Vec::new();
            for child in plan.children() {
                let child = match child.as_any().downcast_ref::<RuntimeGroupExec>() {
                    Some(child) => child.clone(), // Cheap clone.
                    None => return Ok(Transformed::No(plan)),
                };
                children.push(child)
            }

            // Check that all execs have the same runtime preference.
            let preference = match children.get(0) {
                Some(exec) => exec.preference,
                None => return Ok(Transformed::No(plan)),
            };

            // TODO: How do we want to handle "unspecified"? Allow those to run
            // anywhere?
            if !children.iter().all(|exec| exec.preference == preference) {
                return Ok(Transformed::No(plan));
            }

            // All children have the same preference. Swap them out for the
            // actual execs, and replace the node we're currently on with a new
            // runtime group exec.
            let swapped_children: Vec<_> = children.iter().map(|c| c.child.clone()).collect();
            let node = plan.with_new_children(swapped_children)?;

            Ok(Transformed::Yes(Arc::new(RuntimeGroupExec {
                preference,
                child: node,
            })))
        })
    }

    fn name(&self) -> &str {
        "runtime_group_pull_up"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// Whether or not this node is an execution node that we want to pull runtime
/// groups through.
// TODO: This might make more sense excluding our custom DDLs. If we go that
// route, we'll need to move this stuff to sqlexec.
fn can_pull_through_node(plan: &dyn ExecutionPlan) -> bool {
    let plan_any = plan.as_any();
    plan_any.is::<FilterExec>()
        || plan_any.is::<ProjectionExec>()
        || plan_any.is::<HashJoinExec>()
        || plan_any.is::<SortMergeJoinExec>()
        || plan_any.is::<NestedLoopJoinExec>()
        || plan_any.is::<GlobalLimitExec>()
        || plan_any.is::<LocalLimitExec>()
        || plan_any.is::<AggregateExec>()
        || plan_any.is::<AggregateExec>()
        || plan_any.is::<SortExec>()
        || plan_any.is::<SortPreservingMergeExec>()
        || plan_any.is::<InterleaveExec>()
        || plan_any.is::<UnionExec>()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::union::UnionExec;
    use datafusion::{
        arrow::datatypes::Schema,
        physical_plan::{empty::EmptyExec, expressions::Column, filter::FilterExec},
    };
    use protogen::metastore::types::catalog::RuntimePreference;

    use super::*;

    /// Assert two plans are equal by checking explain strings.
    fn assert_plans_equal_str(a: Arc<dyn ExecutionPlan>, b: Arc<dyn ExecutionPlan>) {
        let a = displayable(a.as_ref()).indent(true).to_string();
        let b = displayable(b.as_ref()).indent(true).to_string();
        assert_eq!(a, b)
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("c", DataType::Boolean, false)]))
    }

    #[test]
    fn pull_up_basic() {
        let exec = Arc::new(
            FilterExec::try_new(
                Arc::new(Column::new("c", 0)),
                Arc::new(RuntimeGroupExec::new(
                    RuntimePreference::Remote,
                    Arc::new(EmptyExec::new(true, test_schema())),
                )),
            )
            .unwrap(),
        );

        let out = RuntimeGroupPullUp::new()
            .optimize(exec, &ConfigOptions::default())
            .unwrap();

        let expected = Arc::new(RuntimeGroupExec::new(
            RuntimePreference::Remote,
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Column::new("c", 0)),
                    Arc::new(EmptyExec::new(true, test_schema())),
                )
                .unwrap(),
            ),
        ));

        assert_plans_equal_str(out, expected)
    }

    #[test]
    fn pull_up_multiple_children() {
        let exec = Arc::new(UnionExec::new(vec![
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Column::new("c", 0)),
                    Arc::new(RuntimeGroupExec::new(
                        RuntimePreference::Remote,
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )),
                )
                .unwrap(),
            ),
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Column::new("c", 0)),
                    Arc::new(RuntimeGroupExec::new(
                        RuntimePreference::Remote,
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )),
                )
                .unwrap(),
            ),
        ]));

        let out = RuntimeGroupPullUp::new()
            .optimize(exec, &ConfigOptions::default())
            .unwrap();

        let expected = Arc::new(RuntimeGroupExec::new(
            RuntimePreference::Remote,
            Arc::new(UnionExec::new(vec![
                Arc::new(
                    FilterExec::try_new(
                        Arc::new(Column::new("c", 0)),
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )
                    .unwrap(),
                ),
                Arc::new(
                    FilterExec::try_new(
                        Arc::new(Column::new("c", 0)),
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )
                    .unwrap(),
                ),
            ])),
        ));

        assert_plans_equal_str(out, expected);
    }

    #[test]
    fn partial_pull_up_multiple_children() {
        // Note the differences in runtime preference.
        let exec = Arc::new(UnionExec::new(vec![
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Column::new("c", 0)),
                    Arc::new(RuntimeGroupExec::new(
                        RuntimePreference::Local,
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )),
                )
                .unwrap(),
            ),
            Arc::new(
                FilterExec::try_new(
                    Arc::new(Column::new("c", 0)),
                    Arc::new(RuntimeGroupExec::new(
                        RuntimePreference::Remote,
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )),
                )
                .unwrap(),
            ),
        ]));

        let out = RuntimeGroupPullUp::new()
            .optimize(exec, &ConfigOptions::default())
            .unwrap();

        let expected = Arc::new(UnionExec::new(vec![
            Arc::new(RuntimeGroupExec::new(
                RuntimePreference::Local,
                Arc::new(
                    FilterExec::try_new(
                        Arc::new(Column::new("c", 0)),
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )
                    .unwrap(),
                ),
            )),
            Arc::new(RuntimeGroupExec::new(
                RuntimePreference::Remote,
                Arc::new(
                    FilterExec::try_new(
                        Arc::new(Column::new("c", 0)),
                        Arc::new(EmptyExec::new(true, test_schema())),
                    )
                    .unwrap(),
                ),
            )),
        ]));

        assert_plans_equal_str(out, expected);
    }
}
