use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrl;
use datasources::excel::read_excel_impl;
use ioutil::resolve_path;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::{table_location_and_opts, TableFunc};
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ExcelScan;

impl ConstBuiltinFunction for ExcelScan {
    const DESCRIPTION: &'static str = "Reads an Excel file from the local filesystem";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_excel('file:///path/to/file.xlsx', sheet_name => 'Sheet1')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    const NAME: &'static str = "read_excel";

    fn signature(&self) -> Option<Signature> {
        let options: Fields = vec![
            Field::new("sheet_name", DataType::Utf8, true),
            Field::new("infer_rows", DataType::UInt64, true),
            Field::new("has_header", DataType::Boolean, true),
        ]
        .into_iter()
        .collect();
        Some(Signature::one_of(
            vec![
                TypeSignature::Exact(vec![DataType::Utf8]),
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Struct(options)]),
            ],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ExcelScan {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Local)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, _) = table_location_and_opts(ctx, args, &mut opts)?;

        let url = match source_url {
            DatasourceUrl::File(path) => path,
            DatasourceUrl::Url(url) => {
                return Err(ExtensionError::String(format!(
                    "Expected file, received url: {}",
                    url
                )))
            }
        };

        let url = resolve_path(&url)?;
        let sheet_name: Option<String> = opts
            .remove("sheet_name")
            .map(FuncParamValue::try_into)
            .transpose()?;

        let has_header: Option<bool> = opts
            .remove("has_header")
            .map(FuncParamValue::try_into)
            .transpose()?;

        let infer_schema_len = opts
            .remove("infer_rows")
            .map(FuncParamValue::try_into)
            .transpose()?
            .unwrap_or(100);

        let table = read_excel_impl(&url, sheet_name.as_deref(), has_header, infer_schema_len)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;
        Ok(Arc::new(table))
    }
}
