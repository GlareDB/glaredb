use rayexec_error::Result;

use super::{Materializations, OperatorPlanState};
use crate::execution::operators::copy_to::CopyToOperation;
use crate::execution::operators::sink::PhysicalSink;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_copy::LogicalCopyTo;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_copy_to(
        &mut self,
        materializations: &mut Materializations,
        mut copy_to: Node<LogicalCopyTo>,
    ) -> Result<()> {
        let location = copy_to.location;
        let source = copy_to.take_one_child_exact()?;

        self.walk(materializations, source)?;

        let operator = PhysicalOperator::CopyTo(PhysicalSink::new(CopyToOperation {
            copy_to: copy_to.node.copy_to,
            location: copy_to.node.location,
            schema: copy_to.node.source_schema,
        }));

        self.push_intermediate_operator(operator, location)?;

        Ok(())
    }
}
