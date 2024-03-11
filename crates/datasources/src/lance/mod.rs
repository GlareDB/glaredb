use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use lance::dataset::builder::DatasetBuilder;
use lance::Dataset;
use parser::options::StatementOptions;
use protogen::metastore::types::options::{
    CredentialsOptions,
    StorageOptions,
    TableOptions,
    TableOptionsObjectStore,
    TunnelOptions,
};

use crate::object_store::errors::ObjectStoreSourceError;
use crate::object_store::storage_options_with_credentials;
use crate::{Datasource, DatasourceError};
pub mod insert;

pub struct LanceTable {
    dataset: Dataset,
}

impl LanceTable {
    pub async fn new(location: &str, options: StorageOptions) -> Result<Self> {
        Ok(LanceTable {
            dataset: DatasetBuilder::from_uri(location)
                .with_storage_options(options.inner.into_iter().collect())
                .load()
                .await?,
        })
    }
}

#[async_trait]
impl TableProvider for LanceTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        TableProvider::schema(&self.dataset)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filter: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(TableProvider::scan(&self.dataset, state, projection, filter, limit).await?)
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(self::insert::LanceInsertExecPlan::new(
            self.dataset.clone(),
            input,
        )))
    }
}


pub struct LanceDatasource;

#[async_trait]
impl Datasource for LanceDatasource {
    fn name(&self) -> &'static str {
        "lance"
    }

    /// Create a new datasource from the provided table options and credentials.
    /// CREATE EXTERNAL TABLE foo FROM <name> OPTIONS (...) [CREDENTIALS] (...) [TUNNEL] (...)
    fn table_options_from_stmt(
        &self,
        opts: &mut StatementOptions,
    ) -> Result<TableOptions, DatasourceError> {
        let location: String = opts.remove_required("location")?;
        let storage_options = StorageOptions::try_from(opts)?;

        Ok(TableOptionsObjectStore {
            location,
            storage_options,
            file_type: Some("lance".to_string()),
            compression: None,
            schema_sample_size: None,
        }
        .into())
    }


    async fn create_table_provider(
        &self,
        options: &TableOptions,
        creds: Option<CredentialsOptions>,
        _tunnel_opts: Option<TunnelOptions>,
    ) -> Result<Arc<dyn TableProvider>, DatasourceError> {
        let TableOptionsObjectStore {
            file_type,
            location,
            mut storage_options,
            ..
        } = TableOptionsObjectStore::try_from(options)?;
        if let Some(file_type) = file_type {
            if file_type.as_str() != "lance" {
                return Err(ObjectStoreSourceError::NotSupportFileType(file_type).into());
            }
        }
        if let Some(creds) = creds {
            storage_options_with_credentials(&mut storage_options, creds);
        }
        LanceTable::new(&location, storage_options)
            .await
            .map_err(|e| e.into())
            .map(|t| Arc::new(t) as _)
    }
}
