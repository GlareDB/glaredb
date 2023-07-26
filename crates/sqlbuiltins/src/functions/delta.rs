use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider};
use datasources::common::url::{DatasourceUrl, DatasourceUrlScheme};
use datasources::delta::access::{load_table_direct, DeltaLakeStorageOptions};
use metastore_client::types::options::CredentialsOptions;

/// Function for scanning delta tables.
///
/// Note that this is separate from the other object store functions since
/// initializing object storage happens within the deltalake lib. We're
/// responsible for providing credentials, then it's responsible for creating
/// the store.
///
/// See <https://github.com/delta-io/delta-rs/issues/1521>
#[derive(Debug, Clone, Copy)]
pub struct DeltaScan;

#[async_trait]
impl TableFunc for DeltaScan {
    fn name(&self) -> &str {
        "delta_scan"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (loc, delta_opts) = match args.len() {
            1 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url: String = first.param_into()?;
                let source_url =
                    DatasourceUrl::new(&url).map_err(|e| ExtensionError::Access(Box::new(e)))?;

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, DeltaLakeStorageOptions::Local),
                    other => {
                        return Err(ExtensionError::String(format!(
                            "Credentials required when accessing delta table in {other}"
                        )))
                    }
                }
            }
            2 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url = first.param_into()?;
                let source_url =
                    DatasourceUrl::new(&url).map_err(|e| ExtensionError::Access(Box::new(e)))?;

                let creds: IdentValue = args.next().unwrap().param_into()?;
                let creds = ctx.get_credentials_entry(creds.as_str()).cloned().ok_or(
                    ExtensionError::String(format!("missing credentials object: {creds}")),
                )?;

                match source_url.scheme() {
                    DatasourceUrlScheme::Gcs => match creds.options {
                        CredentialsOptions::Gcp(creds) => {
                            (url, DeltaLakeStorageOptions::Gcs { creds })
                        }
                        other => {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for GCS, got {}",
                                other.as_str()
                            )))
                        }
                    },
                    DatasourceUrlScheme::S3 => {
                        // S3 requires a region parameter.
                        const REGION_KEY: &str = "region";
                        let region = opts
                            .remove(REGION_KEY)
                            .ok_or(ExtensionError::MissingNamedArgument(REGION_KEY))?
                            .param_into()?;

                        match creds.options {
                            CredentialsOptions::Aws(creds) => {
                                (url, DeltaLakeStorageOptions::S3 { creds, region })
                            }
                            other => {
                                return Err(ExtensionError::String(format!(
                                    "invalid credentials for S3, got {}",
                                    other.as_str()
                                )))
                            }
                        }
                    }
                    DatasourceUrlScheme::File => {
                        return Err(ExtensionError::String(
                            "Credentials incorrectly provided when accessing local delta table"
                                .to_string(),
                        ))
                    }
                    DatasourceUrlScheme::Http => {
                        return Err(ExtensionError::String(
                            "Accessing delta tables over http not supported".to_string(),
                        ))
                    }
                }
            }
            _ => return Err(ExtensionError::InvalidNumArgs),
        };

        let table = load_table_direct(&loc, delta_opts)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(table))
    }
}
