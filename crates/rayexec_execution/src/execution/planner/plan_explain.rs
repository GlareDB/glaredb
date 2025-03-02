use rayexec_error::{not_implemented, RayexecError, Result};

use super::{Materializations, OperatorPlanState};
use crate::explain::context_display::ContextDisplayMode;
use crate::explain::explainable::ExplainConfig;
use crate::explain::formatter::ExplainFormatter;
use crate::logical::logical_explain::LogicalExplain;
use crate::logical::operator::Node;

impl OperatorPlanState<'_> {
    pub fn plan_explain(
        &mut self,
        materializations: &mut Materializations,
        mut explain: Node<LogicalExplain>,
    ) -> Result<()> {
        let location = explain.location;

        if self.in_progress.is_some() {
            return Err(RayexecError::new("Expected in progress to be None"));
        }

        if explain.node.analyze {
            not_implemented!("explain analyze")
        }

        // Plan in seperate planner to avoid conmingling pipelines we will be
        // executing (the explain) with the ones we won't (the explained plan
        // itself).
        let input = explain.take_one_child_exact()?;
        let mut planner = Self::new(self.config, self.bind_context);
        // Done in a closure so that we can at least output the logical plans is
        // physical planning errors. This is entirely for dev purposes right now
        // and I expect the conditional will be removed at some point.
        // let plan = || {
        //     planner.walk(materializations, input)?;
        //     planner.finish()?;
        //     Ok::<_, RayexecError>(())
        // };
        // let plan_result = plan();

        // let formatter = ExplainFormatter::new(
        //     self.bind_context,
        //     ExplainConfig {
        //         context_mode: ContextDisplayMode::Enriched(self.bind_context),
        //         verbose: explain.node.verbose,
        //     },
        //     explain.node.format,
        // );

        // let mut type_strings = Vec::new();
        // let mut plan_strings = Vec::new();

        // type_strings.push("unoptimized".to_string());
        // plan_strings.push(formatter.format_logical_plan(&explain.node.logical_unoptimized)?);

        // if let Some(optimized) = explain.node.logical_optimized {
        //     type_strings.push("optimized".to_string());
        //     plan_strings.push(formatter.format_logical_plan(&optimized)?);
        // }

        // match plan_result {
        //     Ok(_) => {
        //         type_strings.push("physical".to_string());
        //         plan_strings.push(formatter.format_intermediate_groups(&[
        //             ("local", &planner.local_group),
        //             ("remote", &planner.remote_group),
        //         ])?);
        //     }
        //     Err(e) => {
        //         error!(%e, "error planning explain input")
        //     }
        // }

        unimplemented!()
        // let physical = Arc::new(PhysicalOperator::Values(PhysicalValues::new(vec![
        //     Batch::try_from_arrays([
        //         Array::from_iter(type_strings),
        //         Array::from_iter(plan_strings),
        //     ])?,
        // ])));

        // let operator = IntermediateOperator {
        //     operator: physical,
        //     partitioning_requirement: None,
        // };

        // self.in_progress = Some(InProgressPipeline {
        //     id: id_gen.next_pipeline_id(),
        //     operators: vec![operator],
        //     location,
        //     source: PipelineSource::InPipeline,
        // });

        // Ok(())
    }
}
