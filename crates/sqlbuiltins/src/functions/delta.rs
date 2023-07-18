use super::*;
use datasources::delta::access::{load_table_direct, DeltaLakeStorageOptions};

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

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[
            TableFuncParameters {
                params: &[TableFuncParameter {
                    name: "url",
                    typ: DataType::Utf8,
                }],
            },
            TableFuncParameters {
                params: &[
                    TableFuncParameter {
                        name: "url",
                        typ: DataType::Utf8,
                    },
                    TableFuncParameter {
                        name: "credentials",
                        typ: DataType::Utf8,
                    },
                ],
            },
        ];

        PARAMS
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (loc, delta_opts) = match args.len() {
            1 => {
                let first = args.get(0).unwrap().clone();
                let url: String = first.param_into()?;
                let source_url = DatasourceUrl::new(&url)?;

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, DeltaLakeStorageOptions::Local),
                    _ => {
                        return Err(BuiltinError::Static(
                            "Credentials required when accessing delta table in S3 or GCS",
                        ))
                    }
                }
            }
            2 => {
                let first = args.get(0).unwrap().clone();
                let url = first.param_into()?;
                let source_url = DatasourceUrl::new(&url)?;
                let creds: IdentValue = args.get(1).unwrap().clone().param_into()?;

                let creds = ctx
                    .get_credentials_entry(creds.as_str())
                    .cloned()
                    .ok_or(BuiltinError::Static("missing credentials object"))?;

                match source_url.scheme() {
                    DatasourceUrlScheme::Gcs => {
                        if let CredentialsOptions::Gcp(creds) = creds.options {
                            (url, DeltaLakeStorageOptions::Gcs { creds })
                        } else {
                            return Err(BuiltinError::Static("invalid credentials for GCS"));
                        }
                    }
                    DatasourceUrlScheme::S3 => {
                        // S3 requires a region parameter.
                        const REGION_KEY: &str = "region";
                        let region = opts
                            .remove(REGION_KEY)
                            .ok_or(BuiltinError::MissingNamedArgument(REGION_KEY))?
                            .param_into()?;

                        if let CredentialsOptions::Aws(creds) = creds.options {
                            (url, DeltaLakeStorageOptions::S3 { creds, region })
                        } else {
                            return Err(BuiltinError::Static("invalid credentials for S3"));
                        }
                    }
                    DatasourceUrlScheme::File => {
                        return Err(BuiltinError::Static(
                            "Credentials incorrectly provided when accessing local delta table",
                        ))
                    }
                    DatasourceUrlScheme::Http => {
                        return Err(BuiltinError::Static(
                            "Accessing delta tables over http not supported",
                        ))
                    }
                }
            }
            _ => return Err(BuiltinError::InvalidNumArgs),
        };

        let table = load_table_direct(&loc, delta_opts)
            .await
            .map_err(|e| BuiltinError::Access(Box::new(e)))?;

        Ok(Arc::new(table))
    }
}
