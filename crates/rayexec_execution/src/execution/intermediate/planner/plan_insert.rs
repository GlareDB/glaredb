use std::sync::Arc;

use rayexec_error::Result;

use super::{IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::execution::operators::insert::InsertOperation;
use crate::execution::operators::sink::PhysicalSink;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_insert::LogicalInsert;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_insert(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut insert: Node<LogicalInsert>,
    ) -> Result<()> {
        let location = insert.location;
        let input = insert.take_one_child_exact()?;

        self.walk(materializations, id_gen, input)?;

        let operator = PhysicalOperator::Insert(PhysicalSink::new(InsertOperation {
            catalog: insert.node.catalog,
            schema: insert.node.schema,
            table: insert.node.table,
        }));

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }
}
