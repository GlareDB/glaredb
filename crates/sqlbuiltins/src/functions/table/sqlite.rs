use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrl;
use datasources::lake::storage_options_into_store_access;
use datasources::object_store::ObjStoreAccessor;
use datasources::sqlite::{SqliteAccess, SqliteTableProvider};
use object_store::ObjectStore;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};
use tempfile;

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
            2 => {
                let mut args = args.into_iter();
                let location: String = args.next().unwrap().try_into()?;
                let table: IdentValue = args.next().unwrap().try_into()?;

                let access = SqliteAccess {
                    db: location.into(),
                    cache: None,
                };
                let state = access.connect().await.map_err(ExtensionError::access)?;
                let provider = SqliteTableProvider::try_new(state, table)
                    .await
                    .map_err(ExtensionError::access)?;
                Ok(Arc::new(provider))
            }
            3 => {
                // TODO: basically all of this needs to be refactored
                // to it's own function so that all the other entry
                // points to the table provider can produce a table provider. It

                let table: IdentValue = args.pop().unwrap().try_into()?;
                let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;
                match source_url {
                    DatasourceUrl::File(location) => {
                        let access = SqliteAccess {
                            db: location,
                            cache: None,
                        };
                        let state = access.connect().await.map_err(ExtensionError::access)?;
                        let provider = SqliteTableProvider::try_new(state, table)
                            .await
                            .map_err(ExtensionError::access)?;
                        Ok(Arc::new(provider))
                    }
                    DatasourceUrl::Url(_) => {
                        let store_access =
                            storage_options_into_store_access(&source_url, &storage_options)
                                .map_err(ExtensionError::access)?;

                        let accessor = ObjStoreAccessor::new(store_access.clone())?;
                        let mut list = accessor.list_globbed(source_url.path()).await?;
                        if list.len() != 1 {
                            return Err(ExtensionError::ObjectStore(format!(
                                "found {} objects matching specification {}",
                                list.len(),
                                source_url
                            )));
                        }
                        let store = store_access.create_store()?;
                        let obj = list.pop().unwrap().location;
                        let payload = store.get(&obj).await?.bytes().await?;
                        let session = ctx.get_session_vars();

                        let tmpdir = Arc::new(
                            tempfile::Builder::new()
                                .prefix(
                                    &vec![
                                        // TODO this path is too long
                                        session.user_name().as_str(),
                                        &session.database_name(),
                                        &session.connection_id().to_string(),
                                    ]
                                    .join(""),
                                )
                                .rand_bytes(8)
                                .tempdir()?,
                        );

                        let tmpdir_path = tmpdir.path();
                        let local_store =
                            object_store::local::LocalFileSystem::new_with_prefix(tmpdir_path)?;

                        let local_path =
                            object_store::path::Path::parse(obj.filename().unwrap_or("sqlite"))?;

                        local_store.put(&local_path, payload).await?;

                        let db = tmpdir_path.join(local_path.filename().unwrap());

                        let access = SqliteAccess {
                            db,
                            cache: Some(tmpdir.clone()),
                        };
                        let state = access.connect().await.map_err(ExtensionError::access)?;
                        Ok(Arc::new(
                            SqliteTableProvider::try_new(state, table)
                                .await
                                .map_err(ExtensionError::access)
                                .unwrap(),
                        ))
                    }
                }
            }
            _ => Err(ExtensionError::String("invalid number of args".to_string())),
        }
    }
}
