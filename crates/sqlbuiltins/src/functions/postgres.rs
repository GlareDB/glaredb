use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFunc, TableFuncContextProvider};
use datasources::postgres::{PostgresAccessor, PostgresTableAccess};

#[derive(Debug, Clone, Copy)]
pub struct ReadPostgres;

#[async_trait]
impl TableFunc for ReadPostgres {
    fn name(&self) -> &str {
        "read_postgres"
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
                let schema: String = args.next().unwrap().param_into()?;
                let table: String = args.next().unwrap().param_into()?;

                let access = PostgresAccessor::connect(&conn_str, None)
                    .await
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        PostgresTableAccess {
                            schema: schema.clone(),
                            name: table.clone(),
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
