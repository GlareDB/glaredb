use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{DFSchema, DFSchemaRef},
    execution::context::SessionState,
    logical_expr::{LogicalPlan, UserDefinedLogicalNode},
    physical_plan::{DisplayAs, ExecutionPlan},
    physical_planner::{ExtensionPlanner, PhysicalPlanner},
};

use crate::planner::{extension_planner::DatafusionExtensionProvider, logical_plan::CreateTable};

#[derive(Debug, Clone)]
pub struct CreateTableExec {
    pub schema: String,
    pub table: String,
    pub if_not_exists: bool,
    pub arrow_schema: DFSchemaRef,
    pub source: Option<Arc<dyn ExecutionPlan>>,
}
pub struct CreateTableExecPlanner;

#[async_trait::async_trait]
impl ExtensionPlanner for CreateTableExecPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::error::Result<Option<Arc<dyn ExecutionPlan>>> {
        let create_table_lp = match node.as_any().downcast_ref::<CreateTable>() {
            Some(s) => s,
            None => todo!(),
        };

        let source = if let Some(source) = &create_table_lp.source {
            planner
                .create_physical_plan(source, session_state)
                .await
                .ok()
        } else {
            None
        };
        let (_, schema, table) = session_state
            .get_session_vars()
            .resolve_table_ref(&create_table_lp.table_name)
            .unwrap();

        let exec = CreateTableExec {
            schema,
            table,
            if_not_exists: create_table_lp.if_not_exists,
            arrow_schema: create_table_lp.schema.clone(),
            source,
        };
        Ok(Some(Arc::new(exec)))
    }
}

impl DisplayAs for CreateTableExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            datafusion::physical_plan::DisplayFormatType::Default => {
                write!(
                    f,
                    "CreateTableExec: schema={}, table={}, if_not_exists={}",
                    self.schema, self.table, self.if_not_exists
                )
            }
            _ => todo!(),
        }
    }
}

impl ExecutionPlan for CreateTableExec {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        let arrow_schema: &DFSchema = self.arrow_schema.as_ref();

        Arc::new(arrow_schema.into())
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        match &self.source {
            Some(source) => vec![source.clone()],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if children.len() > 1 {
            return Err(datafusion::error::DataFusionError::Internal(
                "CreateTableExec::with_new_children: children.len() > 1".to_string(),
            ));
        }
        Ok(Arc::new(CreateTableExec {
            source: children.into_iter().next(),
            ..self.as_ref().clone()
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let _catalog = context.get_session_catalog();
        let vars = context.get_session_vars();
        println!("CreateTableExec::execute: vars={:?}", vars);
        todo!()
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        datafusion::physical_plan::Statistics::default()
    }
}
