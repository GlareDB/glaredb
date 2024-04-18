use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::Result;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadGoogleSheet;

impl ConstBuiltinFunction for ReadGoogleSheet {
    const NAME: &'static str = "read_google_sheet";
    const DESCRIPTION: &'static str = "Reads a csv, tsv or xlsx from google sheets cloud service";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_google_sheet('https://docs.google.com/spreadsheets/example')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    fn signature(&self) -> Option<Signature> {
        // todo:
        Some(Signature::uniform(
            4,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ReadGoogleSheet {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        _args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {

        // todo parse out sheet id from url
        // 'https://docs.google.com/spreadsheets/d/1f5epAPxP_Yd3g1TunEMdtianpVAhKS0RG6BKRDSLtrk/
        // and google sheet api url format: 
        // GET https://sheets.googleapis.com/v4/spreadsheets/SPREADSHEET_ID/values/Sheet1!A1:D3?majorDimension=COLUMNS

        /*
        let provider = GoogleSheetTableProvider::try_new(conn_string, ks, table, user, password)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;
        */

        Ok(Arc::new(None))
    }
}
