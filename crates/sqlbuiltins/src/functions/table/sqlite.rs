use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrl;
use datasources::sqlite::{SqliteAccess, SqliteTableProvider};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::{table_location_and_opts, TableFunc};
use crate::functions::ConstBuiltinFunction;

#[derive(Debug, Clone, Copy)]
pub struct ReadSqlite;

impl ConstBuiltinFunction for ReadSqlite {
    const NAME: &'static str = "read_sqlite";
    const DESCRIPTION: &'static str = "Read a sqlite table";
    const EXAMPLE: &'static str = "SELECT * FROM read_sqlite('/path/to/db.sqlite3', 'table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            2,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for ReadSqlite {
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
        mut args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        // NOTE: the semantics of this are read_sqlite(<path>,
        // <table>) when there are options, the table name is still
        // last. This "feels" wrong to me, but <url> <name> <options>
        // also feels wrong. Putting the table name first feels more
        // consistent between the options, but would require breaking
        // the original API.
        match args.len() {
            0 | 1 => Err(ExtensionError::InvalidNumArgs),
            2 | 3 => {
                let table: IdentValue = args.pop().unwrap().try_into()?;
                let (source_url, mut storage_options) =
                    table_location_and_opts(ctx, args, &mut opts)?;
                let opts = match source_url.clone() {
                    DatasourceUrl::File(_) => None,
                    DatasourceUrl::Url(_) => {
                        let session = ctx.get_session_vars();
                        storage_options.inner.insert(
                            "__tmp_prefix".to_string(),
                            [
                                // TODO this path is too long
                                session.user_name().as_str(),
                                &session.database_name(),
                                &session.connection_id().to_string(),
                            ]
                            .join("")
                            .to_string(),
                        );

                        Some(storage_options)
                    }
                };

                let state = SqliteAccess::new(source_url, opts).await?.connect().await?;

                Ok(Arc::new(SqliteTableProvider::try_new(state, table).await?))
            }
            _ => Err(ExtensionError::String("invalid number of args".to_string())),
        }
    }
}
