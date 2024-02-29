use std::any::Any;
use std::fmt;
use std::sync::Arc;

use datafusion::arrow::array::{GenericStringArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use futures::stream;
use once_cell::sync::Lazy;
use sqlbuiltins::functions::scalars::glaredb_ffi::GlaredbFFIPlugin;
use sqlbuiltins::functions::FunctionRegistry;

use super::install::{get_extension_path, normalize_extension_name};

pub static LOAD_SCHEMA: Lazy<SchemaRef> =
    Lazy::new(|| Schema::new(vec![Field::new("loaded", DataType::Utf8, false)]).into());


#[derive(Debug, Clone)]
pub struct LoadExec {
    pub extension: String,
}

impl ExecutionPlan for LoadExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        LOAD_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Plan(
                "Cannot change children for LoadExec".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "LoadExec only supports 1 partition".to_string(),
            ));
        }
        let stream = stream::once(load_extension(self.clone(), context));
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for LoadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LoadExec")
    }
}

async fn load_extension(
    plan: LoadExec,
    context: Arc<TaskContext>,
) -> DataFusionResult<RecordBatch> {
    let function_registry = context
        .session_config()
        .get_extension::<FunctionRegistry>()
        .unwrap();

    // it's already installed
    if let Some(ext) = get_installed_extension(&plan.extension) {
        let ext = GlaredbFFIPlugin::try_new(&ext).unwrap();
        for func in ext.functions() {
            function_registry.register_udf(func);
        }
    }

    // load it without installing
    if let Some(ext) = normalize_extension_name(&plan.extension) {
        let ext = ext.to_str().ok_or_else(|| {
            DataFusionError::Execution(format!("Failed to get file name from path: {:?}", ext))
        })?;

        let ext = GlaredbFFIPlugin::try_new(ext)?;

        for func in ext.functions() {
            function_registry.register_udf(func);
        }
    }

    let ext_arr = GenericStringArray::<i32>::from(vec![Some(plan.extension.to_string())]);
    let batch = RecordBatch::try_new(plan.schema(), vec![Arc::new(ext_arr)])?;
    Ok(batch)
}

fn get_installed_extension(ext: &str) -> Option<String> {
    let extension_dir = get_extension_path();
    let ext_path = extension_dir.join(ext);
    if ext_path.exists() {
        Some(ext_path.to_str()?.to_string())
    } else {
        None
    }
}
