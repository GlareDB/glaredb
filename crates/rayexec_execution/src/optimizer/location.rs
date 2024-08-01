use crate::logical::operator::{LocationRequirement, LogicalOperator};
use rayexec_error::Result;

use super::OptimizeRule;

/// Rule for pushing down and pulling up location requirements for operators.
///
/// This works by pushing down location requirements as far as possible, then
/// pulling them back up.
///
/// There is no preference for the location requirement for the root of the
/// plan.
#[derive(Debug, Clone)]
pub struct LocationRule {}

impl OptimizeRule for LocationRule {
    fn optimize(&self, mut plan: LogicalOperator) -> Result<LogicalOperator> {
        // TODO: Pull up first.
        plan.walk_mut(
            &mut |op| {
                if op.location() == &LocationRequirement::Any {
                    return Ok(());
                }

                // Push this operator's location down.
                let loc = *op.location();
                op.for_each_child_mut(&mut |child| {
                    if child.location() == &LocationRequirement::Any {
                        *child.location_mut() = loc;
                    }
                    Ok(())
                })
            },
            &mut |op| {
                if op.location() != &LocationRequirement::Any {
                    return Ok(());
                }

                // Set this operator's location from one of the children.
                //
                // For operators where children have different locations, the
                // chosen location is arbitrary.
                let mut loc = None;
                op.for_each_child_mut(&mut |child| {
                    if child.location() != &LocationRequirement::Any {
                        loc = Some(*child.location())
                    }
                    Ok(())
                })?;

                if let Some(loc) = loc {
                    *op.location_mut() = loc;
                }

                Ok(())
            },
        )?;

        Ok(plan)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        database::entry::TableEntry,
        logical::operator::{LogicalNode, Projection, Scan},
    };

    use super::*;

    #[test]
    fn simple_all_local() {
        let plan = LogicalOperator::Projection(LogicalNode::new(Projection {
            exprs: Vec::new(),
            input: Box::new(LogicalOperator::Scan(LogicalNode::with_location(
                Scan {
                    catalog: "catalog".to_string(),
                    schema: "schema".to_string(),
                    source: TableEntry {
                        name: "table".to_string(),
                        columns: Vec::new(),
                    },
                },
                LocationRequirement::ClientLocal,
            ))),
        }));

        let plan = LocationRule {}.optimize(plan).unwrap();
        assert_eq!(&LocationRequirement::ClientLocal, plan.location());
    }

    #[test]
    fn simple_all_remote() {
        let plan = LogicalOperator::Projection(LogicalNode::new(Projection {
            exprs: Vec::new(),
            input: Box::new(LogicalOperator::Scan(LogicalNode::with_location(
                Scan {
                    catalog: "catalog".to_string(),
                    schema: "schema".to_string(),
                    source: TableEntry {
                        name: "table".to_string(),
                        columns: Vec::new(),
                    },
                },
                LocationRequirement::Remote,
            ))),
        }));

        let plan = LocationRule {}.optimize(plan).unwrap();
        assert_eq!(&LocationRequirement::Remote, plan.location());
    }
}
