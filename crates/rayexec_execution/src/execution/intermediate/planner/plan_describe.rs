
use rayexec_error::{RayexecError, Result};

use super::{IntermediatePipelineBuildState, PipelineIdGen};
use crate::execution::operators::values::PhysicalValues;
use crate::execution::operators::PhysicalOperator;
use crate::expr::physical::literal_expr::PhysicalLiteralExpr;
use crate::expr::physical::PhysicalScalarExpression;
use crate::logical::logical_describe::LogicalDescribe;
use crate::logical::operator::Node;

impl IntermediatePipelineBuildState<'_> {
    pub fn plan_describe(
        &mut self,
        id_gen: &mut PipelineIdGen,
        describe: Node<LogicalDescribe>,
    ) -> Result<()> {
        let location = describe.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        let fields = &describe.node.schema.fields;
        let mut row_exprs = Vec::with_capacity(fields.len());

        for field in fields {
            // Push [name, datatype] expressions for each row.
            row_exprs.push(vec![
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: field.name.clone().into(),
                }),
                PhysicalScalarExpression::Literal(PhysicalLiteralExpr {
                    literal: field.datatype.to_string().into(),
                }),
            ]);
        }

        let operator = PhysicalOperator::Values(PhysicalValues::new(row_exprs));

        unimplemented!()
        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: vec![operator],
        //     location,
        //     source: PipelineSource::InPipeline,
        // });

        // Ok(())
    }
}
