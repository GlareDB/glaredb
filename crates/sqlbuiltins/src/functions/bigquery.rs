use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::bigquery::{BigQueryAccessor, BigQueryTableAccess};
use protogen::metastore::types::catalog::RuntimePreference;

#[derive(Debug, Clone, Copy)]
pub struct ReadBigQuery;

#[async_trait]
impl TableFunc for ReadBigQuery {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
    }
    fn name(&self) -> &str {
        "read_bigquery"
    }
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            4,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            4 => {
                let mut args = args.into_iter();
                let service_account: String = args.next().unwrap().try_into()?;
                let project_id: String = args.next().unwrap().try_into()?;
                let dataset_id: String = args.next().unwrap().try_into()?;
                let table_id: String = args.next().unwrap().try_into()?;

                let access = BigQueryAccessor::connect(service_account, project_id)
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        BigQueryTableAccess {
                            dataset_id,
                            table_id,
                        },
                        true,
                    )
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}
