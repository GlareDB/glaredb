use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::mongodb::{MongoAccessor, MongoTableAccessInfo};
use protogen::metastore::types::catalog::RuntimePreference;

use crate::builtins::{BuiltinFunction, TableFunc};

#[derive(Debug, Clone, Copy)]
pub struct ReadMongoDb;

impl BuiltinFunction for ReadMongoDb {
    fn name(&self) -> &str {
        "read_mongodb"
    }

    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            3,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ReadMongoDb {
    fn runtime_preference(&self) -> RuntimePreference {
        RuntimePreference::Remote
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            3 => {
                let mut args = args.into_iter();
                let conn_str: String = args.next().unwrap().param_into()?;
                let database: String = args.next().unwrap().param_into()?;
                let collection: String = args.next().unwrap().param_into()?;

                let access = MongoAccessor::connect(&conn_str)
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_accessor(MongoTableAccessInfo {
                        database,
                        collection,
                    })
                    .into_table_provider()
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(ExtensionError::InvalidNumArgs),
        }
    }
}
