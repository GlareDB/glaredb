use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::ExtensionError;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStore};

use crate::functions::table::{RuntimePreference, TableFunc};
use crate::functions::{ConstBuiltinFunction, FunctionType};

#[derive(Debug, Clone, Copy, Default)]
pub struct GoogleSheets;

impl ConstBuiltinFunction for GoogleSheets {
    const NAME: &'static str = "read_google_sheet";
    const DESCRIPTION: &'static str = "Reads a google sheet.";
    const EXAMPLE: &'static str = "SELECT * FROM read_google_sheet('[key]')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for GoogleSheets {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference, ExtensionError> {
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>, ExtensionError> {
        if args.len() == 0 {
            return Err(ExtensionError::InvalidNumArgs);
        }
        let sheet_id: String = opts
            .get("sheet")
            .map(|val| format!("&sheet={}", val).to_string())
            .unwrap_or_default();

        let key: String = args.into_iter().next().unwrap().try_into()?;
        let url = format!(
            "https://docs.google.com/spreadsheet/ccc?key={}{}&output=csv",
            key, sheet_id
        );

        let stream = reqwest::get(url).await?.bytes_stream().await?;
        let sheet_path = Path::parse("sheet")?;

        let obj_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let  = obj_store.put(&sheet_path, stream).await?;
        let obj_meta = obj_store
            .list(Some(&sheet_path))
            .collect::<Vec<Result<ObjectMeta, _>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let frmt: Box<dyn FileFormat> = Box::new(
            CsvFormat::default()
                .with_schema_infer_max_rec(Some(
                    opts.get("schema_infer_max")
                        .map(|fpv| {
                            i64::try_from(fpv.to_owned()).map_err(|_| {
                                ExtensionError::InvalidParamValue {
                                    param: "schema_infer_max".to_string(),
                                    expected: "integer",
                                }
                            })
                        })
                        .unwrap_or_else(|| Ok(1000))? as usize,
                ))
                .with_has_header(
                    opts.get("has_header")
                        .map(|fpv| bool::try_from(fpv.to_owned()))
                        .unwrap_or_else(|| Ok(true))?,
                )
                .with_delimiter(
                    opts.get("delimiter")
                        .map(|fpv| {
                            String::try_from(fpv.to_owned())
                                .map(|v| v.as_bytes().first().map(|v| v.to_owned()).unwrap_or(b','))
                        })
                        .unwrap_or_else(|| Ok(b','))?,
                )
                .with_quote(
                    opts.get("quote")
                        .map(|fpv| {
                            String::try_from(fpv.to_owned())
                                .map(|v| v.as_bytes().first().map(|v| v.to_owned()).unwrap_or(b'"'))
                        })
                        .unwrap_or_else(|| Ok(b'"'))?,
                ),
        );

        let _schema = frmt
            .infer_schema(&ctx.get_session_state(), &obj_store, obj_meta.as_slice())
            .await?;

        
        panic!()
    }
}
