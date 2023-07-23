use super::*;
use datafusion::arrow::array::StringBuilder;
use datasources::lake::delta::access::load_table_direct;
use datasources::lake::iceberg::table::IcebergTable;
use datasources::lake::LakeStorageOptions;

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
                let source_url = DatasourceUrl::try_new(&url)?;

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, LakeStorageOptions::Local),
                    _ => {
                        return Err(BuiltinError::Static(
                            "Credentials required when accessing delta table in S3 or GCS",
                        ))
                    }
                }
            }
            2 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url = first.param_into()?;
                let source_url = DatasourceUrl::try_new(&url)?;
                let creds: IdentValue = args.next().unwrap().param_into()?;

                let creds = ctx
                    .get_credentials_entry(creds.as_str())
                    .cloned()
                    .ok_or(BuiltinError::Static("missing credentials object"))?;

                match source_url.scheme() {
                    DatasourceUrlScheme::Gcs => {
                        if let CredentialsOptions::Gcp(creds) = creds.options {
                            (url, LakeStorageOptions::Gcs { creds })
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
                            (url, LakeStorageOptions::S3 { creds, region })
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
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (loc, opts) = match args.len() {
            1 => {
                let mut args = args.into_iter();
                let first = args.next().unwrap();
                let url: String = first.param_into()?;
                let source_url = DatasourceUrl::try_new(&url)?;

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, LakeStorageOptions::Local),
                    _ => {
                        return Err(BuiltinError::Static(
                            "Credentials required when accessing delta table in S3 or GCS",
                        ))
                    }
                }
            }
            _ => unimplemented!(),
        };

        let url = DatasourceUrl::try_new(loc)?;
        let store = opts.into_object_store(&url)?;
        let table = IcebergTable::open(url, store).await?;

        let reader = table.table_reader().await?;

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
                let source_url = DatasourceUrl::try_new(&url)?;

                match source_url.scheme() {
                    DatasourceUrlScheme::File => (url, LakeStorageOptions::Local),
                    _ => {
                        return Err(BuiltinError::Static(
                            "Credentials required when accessing delta table in S3 or GCS",
                        ))
                    }
                }
            }
            _ => unimplemented!(),
        };

        let url = DatasourceUrl::try_new(loc)?;
        let store = opts.into_object_store(&url)?;
        let table = IcebergTable::open(url, store).await?;

        unimplemented!()
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
