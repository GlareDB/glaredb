use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrlType;
use datasources::excel::table::ExcelTableProvider;
use datasources::excel::ExcelTable;
use datasources::lake::storage_options_into_store_access;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::{table_location_and_opts, TableFunc};
use crate::functions::table::object_store::urls_from_args;
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ExcelScan;

impl ConstBuiltinFunction for ExcelScan {
    const NAME: &'static str = "read_excel";
    const DESCRIPTION: &'static str = "Reads an Excel file from the local filesystem";
    const EXAMPLE: &'static str =
        "SELECT * FROM read_excel('file:///path/to/file.xlsx', sheet_name => 'Sheet1')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    const ALIASES: &'static [&'static str] = &["read_xlsx"];

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
        args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        let urls = urls_from_args(args)?;
        if urls.is_empty() {
            return Err(ExtensionError::ExpectedIndexedArgument {
                index: 0,
                what: "location of the table".to_string(),
            });
        }

        Ok(match urls.first().unwrap().datasource_url_type() {
            DatasourceUrlType::File => RuntimePreference::Local,
            DatasourceUrlType::Http => RuntimePreference::Remote,
            DatasourceUrlType::Gcs => RuntimePreference::Remote,
            DatasourceUrlType::S3 => RuntimePreference::Remote,
            DatasourceUrlType::Azure => RuntimePreference::Remote,
        })
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;

        let store_access = storage_options_into_store_access(&source_url, &storage_options)
            .map_err(ExtensionError::access)?;

        let sheet_name: Option<String> = opts
            .remove("sheet_name")
            .map(FuncParamValue::try_into)
            .transpose()?;

        let has_header: bool = opts
            .remove("has_header")
            .map(FuncParamValue::try_into)
            .transpose()?
            .unwrap_or(true);

        let infer_num = opts
            .remove("infer_rows")
            .map(FuncParamValue::try_into)
            .transpose()?
            .unwrap_or(100);

        let table = ExcelTable::open(store_access, source_url, sheet_name, has_header)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let provider = ExcelTableProvider::try_new_with_inferred(table, infer_num)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        Ok(Arc::new(provider))
    }
}
