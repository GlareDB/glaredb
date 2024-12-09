use std::sync::Arc;

use rayexec_error::{RayexecError, Result};

use super::{InProgressPipeline, IntermediatePipelineBuildState, Materializations, PipelineIdGen};
use crate::database::create::CreateTableInfo;
use crate::execution::intermediate::pipeline::{IntermediateOperator, PipelineSource};
use crate::execution::operators::create_table::CreateTableSinkOperation;
use crate::execution::operators::empty::PhysicalEmpty;
use crate::execution::operators::sink::SinkOperator;
use crate::execution::operators::PhysicalOperator;
use crate::logical::logical_create::LogicalCreateTable;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_create_table(
        &mut self,
        id_gen: &mut PipelineIdGen,
        materializations: &mut Materializations,
        mut create: Node<LogicalCreateTable>,
    ) -> Result<()> {
        let location = create.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let input: Option<_> = match create.children.len() {
            1 | 0 => create.children.pop(), // Note no unwrap, we want the option here.
            other => {
                return Err(RayexecError::new(format!(
                    "Create table has more than one child: {other}",
                )))
            }
        };

        let is_ctas = input.is_some();
        match input {
            Some(input) => {
                // CTAS, plan the input. It'll be the source of this pipeline.
                self.walk(materializations, id_gen, input)?;
            }
            None => {
                // No input, just have an empty operator as the source.
                let operator = IntermediateOperator {
                    operator: Arc::new(PhysicalOperator::Empty(PhysicalEmpty)),
                    partitioning_requirement: Some(1),
                };

                self.in_progress = Some(InProgressPipeline {
                    id: id_gen.next_pipeline_id(),
                    operators: vec![operator],
                    location,
                    source: PipelineSource::InPipeline,
                });
            }
        };

        let operator = IntermediateOperator {
            operator: Arc::new(PhysicalOperator::CreateTable(SinkOperator::new(
                CreateTableSinkOperation {
                    catalog: create.node.catalog,
                    schema: create.node.schema,
                    info: CreateTableInfo {
                        name: create.node.name,
                        columns: create.node.columns,
                        on_conflict: create.node.on_conflict,
                    },
                    is_ctas,
                },
            ))),
            partitioning_requirement: None,
        };

        self.push_intermediate_operator(operator, location, id_gen)?;

        Ok(())
    }
}
