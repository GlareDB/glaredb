use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider};
use datafusion_ext::local_hint::LocalTableHint;
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::lake::delta::access::load_table_direct;
use datasources::lake::LakeStorageOptions;
use protogen::metastore::types::options::CredentialsOptions;

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
                let source_url = DatasourceUrl::try_new(&url)
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                match source_url.datasource_url_type() {
                    DatasourceUrlType::File => (url, LakeStorageOptions::Local),
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
                let source_url = DatasourceUrl::try_new(&url)
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                let creds: IdentValue = args.next().unwrap().param_into()?;
                let creds = ctx.get_credentials_entry(creds.as_str()).cloned().ok_or(
                    ExtensionError::String(format!("missing credentials object: {creds}")),
                )?;

                match source_url.datasource_url_type() {
                    DatasourceUrlType::Gcs => match creds.options {
                        CredentialsOptions::Gcp(creds) => (url, LakeStorageOptions::Gcs { creds }),
                        other => {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for GCS, got {}",
                                other.as_str()
                            )))
                        }
                    },
                    DatasourceUrlType::S3 => {
                        // S3 requires a region parameter.
                        const REGION_KEY: &str = "region";
                        let region = opts
                            .remove(REGION_KEY)
                            .ok_or(ExtensionError::MissingNamedArgument(REGION_KEY))?
                            .param_into()?;

                        match creds.options {
                            CredentialsOptions::Aws(creds) => {
                                (url, LakeStorageOptions::S3 { creds, region })
                            }
                            other => {
                                return Err(ExtensionError::String(format!(
                                    "invalid credentials for S3, got {}",
                                    other.as_str()
                                )))
                            }
                        }
                    }
                    DatasourceUrlType::File => {
                        return Err(ExtensionError::String(
                            "Credentials incorrectly provided when accessing local delta table"
                                .to_string(),
                        ))
                    }
                    DatasourceUrlType::Http => {
                        return Err(ExtensionError::String(
                            "Accessing delta tables over http not supported".to_string(),
                        ))
                    }
                }
            }
            _ => return Err(ExtensionError::InvalidNumArgs),
        };

        let is_local = matches!(delta_opts, LakeStorageOptions::Local);
        let table = load_table_direct(&loc, delta_opts)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;
        let provider: Arc<dyn TableProvider> = if is_local {
            Arc::new(LocalTableHint(Arc::new(table)))
        } else {
            Arc::new(table)
        };

        Ok(provider)
    }
}
