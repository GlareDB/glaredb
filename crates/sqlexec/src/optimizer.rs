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
                    ExtensionType::CreateTable => {
                        let lp = require_downcast_lp::<CreateTable>(node).clone();
                        let source = lp
                            .source
                            .map(|source| {
                                self.default_optimizer.optimize(&source, config, |_, _| {})
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
                                self.default_optimizer.optimize(&source, config, |_, _| {})
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


#[cfg(test)]
mod test {
    use std::borrow::Cow;
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::logical_expr::{
        col,
        lit,
        table_scan,
        BuiltinScalarFunction,
        Expr,
        Limit,
        Projection,
        ScalarFunctionDefinition,
    };
    use datafusion::optimizer::OptimizerContext;
    use protogen::metastore::types::catalog::RuntimePreference;
    use protogen::metastore::types::options::{
        CopyToDestinationOptions,
        CopyToDestinationOptionsLocal,
        CopyToFormatOptions,
        CopyToFormatOptionsBson,
    };
    use uuid::Uuid;

    use super::*;
    use crate::planner::logical_plan::FullObjectReference;
    use crate::planner::physical_plan::remote_scan::ProviderReference;


    // Create a simple unoptimized plan
    fn create_unoptimized_plan() -> DFLogicalPlan {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);


        let plan = table_scan(Some("test"), &schema, None).unwrap();

        let expr = Expr::ScalarFunction(ScalarFunction {
            func_def: ScalarFunctionDefinition::BuiltIn(BuiltinScalarFunction::Ceil),
            args: vec![col("a")],
        });

        let plan = plan.sort(vec![col("a")]).unwrap();
        let plan = plan.filter(col("b").eq(lit(1))).unwrap();
        let plan = plan.select(vec![0, 1]).unwrap();
        let plan = plan.build().unwrap();
        let plan = DFLogicalPlan::Projection(
            Projection::try_new(
                vec![col("a"), col("b"), expr.alias("a_ceil")],
                Arc::new(plan.clone()),
            )
            .unwrap(),
        );


        DFLogicalPlan::Limit(Limit {
            skip: 0,
            fetch: Some(1),
            input: Arc::new(plan.clone()),
        })
    }

    #[test]
    fn test_ddl_create_table_optimize() {
        let plan = create_unoptimized_plan();
        let schema = plan.schema();

        let optimizer = Optimizer::new();
        let ctx = OptimizerContext::new();

        let optimized_plan = optimizer.optimize(&plan, &ctx, |_, _| {}).unwrap();
        let ddl_plan = DFLogicalPlan::Extension(
            CreateTable {
                tbl_reference: FullObjectReference {
                    database: Cow::Borrowed("default"),
                    schema: Cow::Borrowed("test"),
                    name: Cow::Borrowed("test"),
                },
                if_not_exists: false,
                or_replace: false,
                schema: schema.clone(),
                source: Some(plan),
            }
            .into_extension(),
        );
        let ddl_optimizer = DdlInputOptimizationRule::new();
        let optimized_ddl_plan = ddl_optimizer
            .try_optimize(&ddl_plan, &ctx)
            .unwrap()
            .unwrap();

        let inner = match &optimized_ddl_plan {
            DFLogicalPlan::Extension(ext) => ext
                .node
                .as_ref()
                .as_any()
                .downcast_ref::<CreateTable>()
                .unwrap(),
            _ => panic!("Invalid plan"),
        };

        let optimized_ddl_source = inner.source.as_ref().cloned().unwrap();
        assert_eq!(optimized_ddl_source, optimized_plan);
    }

    #[test]
    fn test_ddl_copy_to_optimize() {
        let plan = create_unoptimized_plan();

        let optimizer = Optimizer::new();
        let ctx = OptimizerContext::new();

        let optimized_plan = optimizer.optimize(&plan, &ctx, |_, _| {}).unwrap();
        let copy_to = DFLogicalPlan::Extension(
            CopyTo {
                source: plan,
                dest: CopyToDestinationOptions::Local(CopyToDestinationOptionsLocal {
                    location: "/tmp".to_string(),
                }),
                format: CopyToFormatOptions::Bson(CopyToFormatOptionsBson {}),
            }
            .into_extension(),
        );
        let ddl_optimizer = DdlInputOptimizationRule::new();
        let optimized_ddl_plan = ddl_optimizer.try_optimize(&copy_to, &ctx).unwrap().unwrap();

        let inner = match &optimized_ddl_plan {
            DFLogicalPlan::Extension(ext) => {
                ext.node.as_ref().as_any().downcast_ref::<CopyTo>().unwrap()
            }
            _ => panic!("Invalid plan"),
        };

        let optimized_ddl_source = inner.source.clone();

        assert_eq!(optimized_ddl_source, optimized_plan);
    }

    #[test]
    fn test_ddl_create_temp_table_optimize() {
        let plan = create_unoptimized_plan();
        let schema = plan.schema();

        let optimizer = Optimizer::new();
        let ctx = OptimizerContext::new();

        let optimized_plan = optimizer.optimize(&plan, &ctx, |_, _| {}).unwrap();
        let ddl_plan = DFLogicalPlan::Extension(
            CreateTempTable {
                tbl_reference: FullObjectReference {
                    database: Cow::Borrowed("default"),
                    schema: Cow::Borrowed("test"),
                    name: Cow::Borrowed("test"),
                },
                if_not_exists: false,
                or_replace: false,
                schema: schema.clone(),
                source: Some(plan),
            }
            .into_extension(),
        );
        let ddl_optimizer = DdlInputOptimizationRule::new();
        let optimized_ddl_plan = ddl_optimizer
            .try_optimize(&ddl_plan, &ctx)
            .unwrap()
            .unwrap();

        let inner = match &optimized_ddl_plan {
            DFLogicalPlan::Extension(ext) => ext
                .node
                .as_ref()
                .as_any()
                .downcast_ref::<CreateTempTable>()
                .unwrap(),
            _ => panic!("Invalid plan"),
        };

        let optimized_ddl_source = inner.source.as_ref().cloned().unwrap();
        assert_eq!(optimized_ddl_source, optimized_plan);
    }

    #[test]
    fn test_ddl_insert_optimize() {
        let plan = create_unoptimized_plan();

        let optimizer = Optimizer::new();
        let ctx = OptimizerContext::new();

        let optimized_plan = optimizer.optimize(&plan, &ctx, |_, _| {}).unwrap();
        let ddl_plan = DFLogicalPlan::Extension(
            Insert {
                source: plan,
                provider: ProviderReference::RemoteReference(Uuid::nil()),
                runtime_preference: RuntimePreference::Unspecified,
            }
            .into_extension(),
        );
        let ddl_optimizer = DdlInputOptimizationRule::new();
        let optimized_ddl_plan = ddl_optimizer
            .try_optimize(&ddl_plan, &ctx)
            .unwrap()
            .unwrap();

        let inner = match &optimized_ddl_plan {
            DFLogicalPlan::Extension(ext) => {
                ext.node.as_ref().as_any().downcast_ref::<Insert>().unwrap()
            }
            _ => panic!("Invalid plan"),
        };

        let optimized_ddl_source = inner.source.clone();
        assert_eq!(optimized_ddl_source, optimized_plan);
    }
}
