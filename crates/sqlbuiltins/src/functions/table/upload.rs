use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};
use tracing::debug;

use crate::functions::ConstBuiltinFunction;
use crate::functions::table::TableFunc;

#[derive(Debug, Clone, Copy, Default)]
pub struct GlareUpload;

impl ConstBuiltinFunction for GlareUpload {
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
impl TableFunc for GlareUpload {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // Uploads can only exist remotely; this operation is not meaningful
        // when not connected to remote/hybrid.
        //
        // TODO: helpful error message if attempting to run local
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        _ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            1 => {
                let mut args = args.into_iter();
                let fname: String = args.next().unwrap().try_into()?;
                debug!("{}", fname); // TODO: remove me

                unimplemented!("TODO") // TODO: implement
            }
            _ => Err(ExtensionError::InvalidNumArgs)
        }
    }
}
