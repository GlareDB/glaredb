use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider};
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::lake::delta::access::load_table_direct;
use object_store::{aws::AmazonS3ConfigKey, gcp::GoogleConfigKey};
use protogen::metastore::types::catalog::RuntimePreference;
use protogen::metastore::types::options::{CredentialsOptions, StorageOptions};

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
    fn runtime_preference(&self) -> RuntimePreference {
        // TODO: Detect runtime.
        RuntimePreference::Remote
    }

    fn name(&self) -> &str {
        "delta_scan"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let mut args = args.into_iter();
        let first = args.next().unwrap();
        let url: String = first.param_into()?;
        let source_url =
            DatasourceUrl::try_new(&url).map_err(|e| ExtensionError::Access(Box::new(e)))?;

        let mut maybe_cred_opts = None;
        // Check if a credentials object has been supplied
        if let Some(func_param) = args.next() {
            let creds: IdentValue = func_param.param_into()?;
            maybe_cred_opts = Some(
                ctx.get_credentials_entry(creds.as_str())
                    .cloned()
                    .ok_or(ExtensionError::String(format!(
                        "missing credentials object: {creds}"
                    )))?
                    .options,
            );
        }

        let mut storage_options = StorageOptions::default();
        match (source_url.datasource_url_type(), maybe_cred_opts) {
            (DatasourceUrlType::File, None) => {} // no options fine in this case
            (DatasourceUrlType::File, _) => {
                return Err(ExtensionError::String(
                    "Credentials incorrectly provided when accessing local delta table".to_string(),
                ))
            }
            (DatasourceUrlType::Gcs, Some(CredentialsOptions::Gcp(creds))) => {
                storage_options.inner.insert(
                    GoogleConfigKey::ServiceAccountKey.as_ref().to_string(),
                    creds.service_account_key,
                );
            }
            (DatasourceUrlType::S3, Some(CredentialsOptions::Aws(creds))) => {
                const REGION_KEY: &str = "region";
                let region = opts
                    .remove(REGION_KEY)
                    .ok_or(ExtensionError::MissingNamedArgument(REGION_KEY))?
                    .param_into()?;

                storage_options.inner.insert(
                    AmazonS3ConfigKey::AccessKeyId.as_ref().to_string(),
                    creds.access_key_id,
                );
                storage_options.inner.insert(
                    AmazonS3ConfigKey::SecretAccessKey.as_ref().to_string(),
                    creds.secret_access_key,
                );
                storage_options
                    .inner
                    .insert(AmazonS3ConfigKey::Region.as_ref().to_string(), region);
            }
            (DatasourceUrlType::Http, _) => {
                return Err(ExtensionError::String(
                    "Accessing delta tables over http not supported".to_string(),
                ))
            }
            (datasource, creds) => {
                return Err(ExtensionError::String(format!(
                    "Invalid credentials for {datasource}, got {} creds",
                    if let Some(o) = creds {
                        o.as_str()
                    } else {
                        "no"
                    }
                )))
            }
        };

        let table = load_table_direct(&url, storage_options)
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;

        Ok(Arc::new(table))
    }
}
