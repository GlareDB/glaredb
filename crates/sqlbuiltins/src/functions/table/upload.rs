use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};
use tracing::debug;

use crate::functions::table::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy, Default)]
pub struct GlareDBUpload;

impl ConstBuiltinFunction for GlareDBUpload {
    const NAME: &'static str = "glaredb_upload";
    const DESCRIPTION: &'static str = "Reads a file that was uploaded to GlareDB Cloud.";
    const EXAMPLE: &'static str = "SELECT * FROM glaredb_upload('my_upload.csv')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    // signature for GlareUpload is a single filename. The filename may
    // optionally contain an extension, though it is not required. Filename
    // should not be a path.
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            1,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for GlareDBUpload {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // Uploads can only exist remotely; this operation is not meaningful
        // when not connected to remote/hybrid.
        //
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let obj_name: String = args.next().unwrap().try_into()?;

        let storage = ctx
            .get_session_state()
            .config()
            .get_extension::<NativeTableStorage>()
            .ok_or_else(|| {
                ExtensionError::String(
                    format!(
                        "access unavailable, {} is not supported in local environments",
                        self.NAME
                    )
                    .to_string(),
                )
            })?;


	let boo = ctx.get_session_state().table_factories().get("read_csv").unwrap();

        // TODO:
        //  - build the url off of the filename, bucket, and db/user/prefix -> DatasourceUrl
        //  - get function ref for infer_func_for_file
	//  - get the "real" table function somehow. from somewhere

        Err(ExtensionError::new())
    }
}

struct CloudUploadTable {
    obj_name: String,
}

#[async_trait]
impl TableProvider for CloudUploadTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {}

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filter: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        todo!()
    }
}

struct CloudUploadExec;

impl ExecutionPlan for CloudUploadExec {}
