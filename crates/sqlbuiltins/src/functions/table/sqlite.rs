use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Signature, Volatility};
use datafusion::parquet::data_type::AsBytes;
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
        args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Local)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            0 | 1 => Err(ExtensionError::InvalidNumArgs),
            2 => {
                let mut args = args.into_iter();
                let location: String = args.next().unwrap().try_into()?;
                let table: IdentValue = args.next().unwrap().try_into()?;

                let access = SqliteAccess {
                    db: location.into(),
                };
                let state = access.connect().await.map_err(ExtensionError::access)?;
                let provider = SqliteTableProvider::try_new(state, table)
                    .await
                    .map_err(ExtensionError::access)?;
                Ok(Arc::new(provider))
            }
            2 | 3 => {
                let table: IdentValue = args.pop().unwrap().try_into()?;

                let (source_url, storage_options) = table_location_and_opts(ctx, args, &mut opts)?;
                match source_url {
                    DatasourceUrl::File(location) => {
                        let access = SqliteAccess { db: location };
                        let state = access.connect().await.map_err(ExtensionError::access)?;
                        let provider = SqliteTableProvider::try_new(state, table)
                            .await
                            .map_err(ExtensionError::access)?;
                        Ok(Arc::new(provider))
                    }
                    DatasourceUrl::Url(uri) => {
                        let store_access =
                            storage_options_into_store_access(&source_url, &storage_options)
                                .map_err(ExtensionError::access)?;

                        let accessor = ObjStoreAccessor::new(store_access)?;
                        let list = accessor.list_globbed(source_url.path()).await?;
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
                        // TODO this is too long
                        let mut prefix = &session.user_name().into_bytes();
                        prefix.extend(session.database_name().bytes());
                        prefix.extend(session.connection_id().into_bytes());
                        let prefix: &str = prefix.into_iter().collect();
                        let tmpdir = tempfile::Builder::new()
                            .prefix(prefix)
                            .rand_bytes(8)
                            .tempdir()?;

                        let local_store =
                            object_store::local::LocalFileSystem::new_with_prefix(tmpdir)?;


                        let local_path =
                            object_store::path::Path::parse(obj.filename().unwrap_or("sqlite"))?;

                        let res = local_store.put(&local_path, payload).await?;

                        let db = tmpdir.into_path().join(local_path.filename().unwrap());


                        let access = SqliteAccess { db };
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
        }
    }
}
