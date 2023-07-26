use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider};
use datasources::common::url::{DatasourceUrl, DatasourceUrlScheme};
use datasources::lake::iceberg::table::IcebergTable;
use datasources::lake::LakeStorageOptions;
use metastore_client::types::options::CredentialsOptions;

#[derive(Debug, Clone, Copy)]
pub struct IcebergScan;

#[async_trait]
impl TableFunc for IcebergScan {
    fn name(&self) -> &str {
        "iceberg_scan"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        // TODO: Reduce duplication
        let (loc, opts) = match args.len() {
            1 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url: String = first.param_into()?;
                let source_url = DatasourceUrl::try_new(&url).unwrap();

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, LakeStorageOptions::Local),
                    _ => {
                        return Err(ExtensionError::String(format!(
                            "Credentials required when accessing delta table in S3 or GCS",
                        )))
                    }
                }
            }
            2 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url = first.param_into()?;
                let source_url = DatasourceUrl::try_new(&url).unwrap();
                let creds: IdentValue = args.next().unwrap().param_into()?;

                let creds = ctx.get_credentials_entry(creds.as_str()).cloned().ok_or(
                    ExtensionError::String(format!("missing credentials object")),
                )?;

                match source_url.scheme() {
                    DatasourceUrlScheme::Gcs => {
                        if let CredentialsOptions::Gcp(creds) = creds.options {
                            (url, LakeStorageOptions::Gcs { creds })
                        } else {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for GCS"
                            )));
                        }
                    }
                    DatasourceUrlScheme::S3 => {
                        // S3 requires a region parameter.
                        const REGION_KEY: &str = "region";
                        let region = opts
                            .remove(REGION_KEY)
                            .ok_or(ExtensionError::MissingNamedArgument(REGION_KEY))?
                            .param_into()?;

                        if let CredentialsOptions::Aws(creds) = creds.options {
                            (url, LakeStorageOptions::S3 { creds, region })
                        } else {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for S3"
                            )));
                        }
                    }
                    DatasourceUrlScheme::File => {
                        return Err(ExtensionError::String(format!(
                            "Credentials incorrectly provided when accessing local iceberg table",
                        )))
                    }
                    DatasourceUrlScheme::Http => {
                        return Err(ExtensionError::String(format!(
                            "Accessing iceberg tables over http not supported",
                        )))
                    }
                }
            }
            _ => return Err(ExtensionError::InvalidNumArgs),
        };

        let url = DatasourceUrl::try_new(loc).unwrap();
        let store = opts.into_object_store(&url).unwrap();
        let table = IcebergTable::open(url, store).await.unwrap();

        let reader = table.table_reader().await.unwrap();

        Ok(reader)
    }
}

/// Function for getting iceberg table metadata.
#[derive(Debug, Clone, Copy)]
pub struct IcebergMetadata;

#[async_trait]
impl TableFunc for IcebergMetadata {
    fn name(&self) -> &str {
        "iceberg_metadata"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (loc, opts) = match args.len() {
            1 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url: String = first.param_into()?;
                let source_url = DatasourceUrl::try_new(&url)
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, LakeStorageOptions::Local),
                    _ => {
                        return Err(ExtensionError::String(format!(
                            "Credentials required when accessing delta table in S3 or GCS",
                        )))
                    }
                }
            }
            _ => unimplemented!(),
        };

        unimplemented!()

        // let url = DatasourceUrl::try_new(loc)?;
        // let store = opts.into_object_store(&url)?;
        // let table = IcebergTable::open(url, store).await?;

        // let serialized = serde_json::to_string_pretty(table.metadata()).unwrap();

        // let mut builder = StringBuilder::new();
        // builder.append_value(serialized);

        // let schema = Arc::new(Schema::new(vec![Field::new(
        //     "metadata",
        //     DataType::Utf8,
        //     false,
        // )]));

        // let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(builder.finish())]).unwrap();

        // Ok(Arc::new(
        //     MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
        // ))
    }
}
