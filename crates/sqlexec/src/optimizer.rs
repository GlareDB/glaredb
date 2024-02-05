use datafusion::logical_expr::{LogicalPlan as DFLogicalPlan, UserDefinedLogicalNode};
use datafusion::optimizer::optimizer::Optimizer;
use datafusion::optimizer::OptimizerRule;

use crate::planner::extension::{ExtensionNode, ExtensionType};
use crate::planner::logical_plan::{CopyTo, CreateTable, CreateTempTable, Insert};


fn require_downcast_lp<P: 'static>(plan: &dyn UserDefinedLogicalNode) -> &P {
    match plan.as_any().downcast_ref::<P>() {
        Some(p) => p,
        None => panic!("Invalid downcast reference for plan: {}", plan.name()),
    }
}

pub(crate) struct DdlInputOptimizationRule {
    default_optimizer: Optimizer,
}
impl DdlInputOptimizationRule {
    pub fn new() -> Self {
        Self {
            default_optimizer: Optimizer::new(),
        }
    }
}

impl OptimizerRule for DdlInputOptimizationRule {
    fn try_optimize(
        &self,
        plan: &DFLogicalPlan,
        config: &dyn datafusion::optimizer::OptimizerConfig,
    ) -> datafusion::error::Result<Option<DFLogicalPlan>> {
        match plan {
            DFLogicalPlan::Extension(ext) => {
                let node = ext.node.as_ref();
                let extension_type = node.name().parse::<ExtensionType>().unwrap();
                match extension_type {
                    ExtensionType::CopyTo => {
                        let lp = require_downcast_lp::<CopyTo>(node).clone();
                        let source =
                            self.default_optimizer
                                .optimize(&lp.source, config, |_, _| {})?;

                        let lp = CopyTo { source, ..lp };

                        Ok(Some(DFLogicalPlan::Extension(lp.into_extension())))
                    }
                    ExtensionType::CreateExternalTable => {
                        let lp = require_downcast_lp::<CreateTable>(node).clone();
                        let source = lp
                            .source
                            .map(|source| {
                                let o = self.default_optimizer.optimize(&source, config, |_, _| {});
                                o
                            })
                            .transpose()?;

                        let lp = CreateTable { source, ..lp };
                        Ok(Some(DFLogicalPlan::Extension(lp.into_extension())))
                    }
                    ExtensionType::CreateTable => {
                        let lp = require_downcast_lp::<CreateTable>(node).clone();
                        let source = lp
                            .source
                            .map(|source| {
                                let o = self.default_optimizer.optimize(&source, config, |_, _| {});
                                o
                            })
                            .transpose()?;

                        let lp = CreateTable { source, ..lp };
                        Ok(Some(DFLogicalPlan::Extension(lp.into_extension())))
                    }
                    ExtensionType::CreateTempTable => {
                        let lp = require_downcast_lp::<CreateTempTable>(node).clone();
                        let source = lp
                            .source
                            .map(|source| {
                                let o = self.default_optimizer.optimize(&source, config, |_, _| {});
                                o
                            })
                            .transpose()?;

                        let lp = CreateTempTable { source, ..lp };
                        Ok(Some(DFLogicalPlan::Extension(lp.into_extension())))
                    }
                    ExtensionType::Insert => {
                        let lp = require_downcast_lp::<Insert>(node).clone();
                        let source =
                            self.default_optimizer
                                .optimize(&lp.source, config, |_, _| {})?;

                        let lp = Insert { source, ..lp };
                        Ok(Some(DFLogicalPlan::Extension(lp.into_extension())))
                    }
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    fn name(&self) -> &str {
        "DdlInputOptimizationRule"
    }
}
