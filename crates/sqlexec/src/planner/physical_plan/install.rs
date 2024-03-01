use std::any::Any;
use std::env::consts::{DLL_EXTENSION, DLL_PREFIX, DLL_SUFFIX};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{fmt, fs};

use datafusion::arrow::array::GenericStringArray;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
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
use datafusion_ext::vars::SessionVars;
use futures::stream;

use crate::planner::errors::PlanError;
#[derive(Debug, Clone)]
pub struct InstallExec {
    pub extension: String,
}

impl ExecutionPlan for InstallExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "extension",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        )]))
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
                "Cannot change children for InstallExec".to_string(),
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
                "InstallExec only supports 1 partition".to_string(),
            ));
        }

        let vars = context
            .session_config()
            .options()
            .extensions
            .get::<SessionVars>()
            .expect("context should have SessionVars extension");

        // InstallExec is exclusively for local/standalone instances
        // This is a bit redundant as it's already checked during planning, but this serves as an
        // extra layer of protection in case someone tries to bypass the planner
        if vars.is_server_instance() {
            return Err(
                PlanError::UnsupportedFeature("installing extensions on remote instances").into(),
            );
        } else if vars.is_cloud_instance() {
            return Err(
                PlanError::UnsupportedFeature("installing extensions on cloud instances").into(),
            );
        }

        let extension_dir = vars.extension_dir();
        let this = self.clone();
        let stream = stream::once(this.install_extension(extension_dir));

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for InstallExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "InstallExec")
    }
}

impl InstallExec {
    async fn install_extension(self, extension_dir: String) -> DataFusionResult<RecordBatch> {
        let output_schema = self.schema();
        let extension_dir = PathBuf::from(extension_dir);
        let extension = self.extension;

        if let Some(path) = normalize_extension_name(&extension) {
            let mut ext_name = path.file_name().and_then(|s| s.to_str()).ok_or_else(|| {
                DataFusionError::Execution(format!("Failed to get file name from path: {:?}", path))
            })?;
            if ext_name.starts_with(DLL_PREFIX) {
                ext_name = &ext_name[DLL_PREFIX.len()..];
            }
            if ext_name.ends_with(DLL_SUFFIX) {
                ext_name = &ext_name[..ext_name.len() - DLL_SUFFIX.len()];
            }

            let dest = extension_dir.join(ext_name);

            if !dest.exists() {
                fs::create_dir_all(&extension_dir).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to create extension directory: {:?}",
                        e
                    ))
                })?;
                fs::copy(&path, dest).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to copy extension to directory: {:?}",
                        e
                    ))
                })?;
            }
            let ext_arr = GenericStringArray::<i32>::from(vec![Some(ext_name.to_string())]);
            let batch = RecordBatch::try_new(output_schema, vec![Arc::new(ext_arr)])?;
            Ok(batch)
        } else {
            Ok(RecordBatch::new_empty(output_schema))
        }
    }
}

/// returns some path if the extension name is valid and the file exists
pub(super) fn normalize_extension_name(name: &str) -> Option<PathBuf> {
    let mut maybe_path = Path::new(&name).to_path_buf();
    if !maybe_path.ends_with(DLL_SUFFIX) {
        maybe_path.set_extension(DLL_EXTENSION);
    };
    std::fs::canonicalize(maybe_path).ok()
}
