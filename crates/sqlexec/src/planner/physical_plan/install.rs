use std::any::Any;
use std::env::consts::{ARCH, DLL_EXTENSION, DLL_PREFIX, DLL_SUFFIX, FAMILY};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{env, fmt, fs};

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
use futures::stream;
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
        _: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "InstallExec only supports 1 partition".to_string(),
            ));
        }
        let this = self.clone();
        let stream = stream::once(this.install_extension());

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
    async fn install_extension(self) -> DataFusionResult<RecordBatch> {
        let output_schema = self.schema();
        let extension_dir = get_extension_path();
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


pub(super) fn get_home_dir() -> PathBuf {
    match env::var("HOME") {
        Ok(path) => PathBuf::from(path),
        Err(_) => match env::var("USERPROFILE") {
            Ok(path) => PathBuf::from(path),
            Err(_) => panic!("Failed to get home directory"),
        },
    }
}

pub(super) fn get_extension_path() -> PathBuf {
    let mut home_dir = get_home_dir();
    home_dir.push(".glaredb");
    home_dir.push("extensions");
    home_dir.push(env!("CARGO_PKG_VERSION"));
    // push the arch
    home_dir.push(format!("{}_{}", FAMILY, ARCH));
    home_dir
}
// /Users/corygrinstead/.glaredb/extensions/0.9.0/unix_aarch64/glaredb_distance
// /Users/corygrinstead/.glaredb/extensions/0.9.0/unix_aarch64/glaredb_distance