use std::collections::HashMap;
use std::marker::PhantomData;
// use std::path::Path;
use std::sync::Arc;
use std::vec;

use async_trait::async_trait;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::TableProvider;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, IdentValue, TableFuncContextProvider};
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::native::access::NativeTableStorage;
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::generic::GenericStoreAccess;
use datasources::object_store::http::HttpStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::{MultiSourceTableProvider, ObjStoreAccess, ObjStoreTableProvider};
use futures::{StreamExt, TryStreamExt};
use object_store::azure::AzureConfigKey;
use object_store::path::Path as ObjectStorePath;
use object_store::{ObjectMeta, ObjectStore};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};
use protogen::metastore::types::options::{CredentialsOptions, StorageOptions};

use crate::functions::{BuiltinFunction, ConstBuiltinFunction, TableFunc};

#[derive(Debug, Clone, Copy)]
pub struct ParquetOptionsReader;

impl OptionReader for ParquetOptionsReader {
    type Format = ParquetFormat;

    const OPTIONS: &'static [(&'static str, DataType)] = &[];

    fn read_options(_opts: &HashMap<String, FuncParamValue>) -> Result<Self::Format> {
        Ok(ParquetFormat::default())
    }
}

pub const READ_PARQUET: ObjScanTableFunc<ParquetOptionsReader> = ObjScanTableFunc {
    name: "read_parquet",
    aliases: &["parquet_scan"],
    description: "Returns a table by scanning the given Parquet file(s).",
    example: "SELECT * FROM read_parquet('./my_data.parquet')",
    phantom: PhantomData,
};

#[derive(Debug, Clone, Copy)]
pub struct CsvOptionReader;

impl OptionReader for CsvOptionReader {
    type Format = CsvFormat;

    const OPTIONS: &'static [(&'static str, DataType)] = &[
        // Specify delimiter between fields. Default: ','
        ("delimiter", DataType::Utf8),
        // Try to read a header. Default: true
        ("has_header", DataType::Boolean),
    ];

    fn read_options(opts: &HashMap<String, FuncParamValue>) -> Result<Self::Format> {
        let mut format = CsvFormat::default().with_schema_infer_max_rec(Some(20480));

        if let Some(delimiter) = opts.get("delimiter") {
            let delimiter: String = delimiter.clone().try_into()?;
            let bs = delimiter.as_bytes();
            if bs.len() != 1 {
                return Err(ExtensionError::String(
                    "delimiters for CSV must fit in one byte (e.g. ',')".to_string(),
                ));
            }
            let delimiter = bs[0];
            format = format.with_delimiter(delimiter);
        }

        if let Some(header) = opts.get("has_header") {
            let has_header: bool = header.clone().try_into()?;
            format = format.with_has_header(has_header);
        }

        Ok(format)
    }
}

pub const READ_CSV: ObjScanTableFunc<CsvOptionReader> = ObjScanTableFunc {
    name: "read_csv",
    aliases: &["csv_scan"],
    description: "Returns a table by scanning the given CSV file(s).",
    example: "SELECT * FROM read_csv('./my_data.csv')",
    phantom: PhantomData,
};

#[derive(Debug, Clone, Copy)]
pub struct JsonOptionsReader;

impl OptionReader for JsonOptionsReader {
    type Format = JsonFormat;

    const OPTIONS: &'static [(&'static str, DataType)] = &[];

    fn read_options(_opts: &HashMap<String, FuncParamValue>) -> Result<Self::Format> {
        Ok(JsonFormat::default())
    }
}

pub const READ_JSON: ObjScanTableFunc<JsonOptionsReader> = ObjScanTableFunc {
    name: "read_ndjson",
    aliases: &["ndjson_scan"],
    description: "Returns a table by scanning the given JSON file(s).",
    example: "SELECT * FROM read_ndjson('./my_data.json')",
    phantom: PhantomData,
};

pub trait OptionReader: Sync + Send + Sized {
    type Format: FileFormat + WithCompression + 'static;

    /// List of options and their expected data types.
    const OPTIONS: &'static [(&'static str, DataType)];

    /// Read user provided options, and construct a file format using those options.
    fn read_options(opts: &HashMap<String, FuncParamValue>) -> Result<Self::Format>;
}

/// Helper trait for adding the compression option to file formats.
pub trait WithCompression: Sized {
    fn with_compression(self, compression: FileCompressionType) -> Result<Self>;
}

impl WithCompression for CsvFormat {
    fn with_compression(self, compression: FileCompressionType) -> Result<Self> {
        Ok(CsvFormat::with_file_compression_type(self, compression))
    }
}

impl WithCompression for JsonFormat {
    fn with_compression(self, compression: FileCompressionType) -> Result<Self> {
        Ok(JsonFormat::with_file_compression_type(self, compression))
    }
}

impl WithCompression for ParquetFormat {
    fn with_compression(self, _compression: FileCompressionType) -> Result<Self> {
        // TODO: Snappy is a common compression algo to use parquet. If we want
        // to support it, we'd need to extend the file compression enum with our
        // own version.
        Err(ExtensionError::String(
            "compression not supported for parquet".to_string(),
        ))
    }
}

/// Generic file scan for different file types.
#[derive(Debug, Clone)]
pub struct ObjScanTableFunc<Opts> {
    /// Primary name for the function.
    name: &'static str,

    /// Additional aliases for this function.
    aliases: &'static [&'static str],

    description: &'static str,
    example: &'static str,

    phantom: PhantomData<Opts>,
}

impl<Opts: OptionReader> BuiltinFunction for ObjScanTableFunc<Opts> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn aliases(&self) -> &'static [&'static str] {
        self.aliases
    }

    fn function_type(&self) -> FunctionType {
        FunctionType::TableReturning
    }

    fn sql_example(&self) -> Option<&str> {
        Some(self.example)
    }

    fn description(&self) -> Option<&str> {
        Some(self.description)
    }

    fn signature(&self) -> Option<Signature> {
        let opts: Fields = Opts::OPTIONS
            .iter()
            .map(|opt| Field::new(opt.0, opt.1.clone(), false))
            .collect();

        Some(Signature::one_of(
            vec![
                // read_csv('path')
                TypeSignature::Exact(vec![DataType::Utf8]),
                // read_csv('path', ...options)
                TypeSignature::Exact(vec![DataType::Utf8, DataType::Struct(opts.clone())]),
                // read_csv(['path1', 'path2'])
                TypeSignature::Exact(vec![DataType::new_list(DataType::Utf8, false)]),
                // read_csv(['path1', 'path2'], options)
                TypeSignature::Exact(vec![
                    DataType::new_list(DataType::Utf8, false),
                    DataType::Struct(opts),
                ]),
            ],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl<Opts: OptionReader> TableFunc for ObjScanTableFunc<Opts> {
    fn detect_runtime(
        &self,
        args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        let urls = urls_from_args(args)?;
        // All urls are of the same type, just need to get the runtime from the
        // first.
        Ok(match urls.first().unwrap().datasource_url_type() {
            DatasourceUrlType::File => RuntimePreference::Local,
            DatasourceUrlType::Http => RuntimePreference::Remote,
            DatasourceUrlType::Gcs => RuntimePreference::Remote,
            DatasourceUrlType::S3 => RuntimePreference::Remote,
            DatasourceUrlType::Azure => RuntimePreference::Remote,
        })
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let urls = urls_from_args(&args)?;
        let creds_ident = self.credentials_from_args(&args)?;

        // Read in user provided options and use them to construct the format.
        let mut format = Opts::read_options(&opts)?;

        // Read in compression is provided by the user, or try to infer it from
        // the file extension.
        let compression: Option<FileCompressionType> = match opts.remove("compression") {
            Some(cmp) => {
                let cmp: String = cmp.try_into()?;
                Some(cmp.parse::<FileCompressionType>()?)
            }
            None => {
                let path = urls.first().expect("non-empty urls").path();
                let path = std::path::Path::new(path.as_ref());
                path.extension()
                    .and_then(|ext| ext.to_string_lossy().as_ref().parse().ok())
            }
        };

        if let Some(compression) = compression {
            format = format.with_compression(compression)?;
        }

        // Optimize creating a table provider for objects by clubbing the same
        // store together.
        let mut fn_registry: HashMap<
            ObjectStoreUrl,
            (Arc<dyn ObjStoreAccess>, Vec<DatasourceUrl>),
        > = HashMap::new();
        for source_url in urls {
            let access = get_store_access(ctx, &source_url, creds_ident.as_ref(), opts.clone())?;
            let base_url = access
                .base_url()
                .map_err(|e| ExtensionError::Access(Box::new(e)))?;

            if let Some((_, locations)) = fn_registry.get_mut(&base_url) {
                locations.push(source_url);
            } else {
                fn_registry.insert(base_url, (access, vec![source_url]));
            }
        }

        // Now that we have all urls (grouped by their access), we try and get
        // all the objects and turn this into a table provider.

        let format: Arc<dyn FileFormat> = Arc::new(format);
        let table = fn_registry
            .into_values()
            .map(|(access, locations)| {
                let provider =
                    get_table_provider(ctx, format.clone(), access, locations.into_iter());
                provider
            })
            .collect::<futures::stream::FuturesUnordered<_>>()
            .try_collect::<Vec<_>>()
            .await
            .map(|providers| Arc::new(MultiSourceTableProvider::new(providers)) as _)?;

        Ok(table)
    }
}

/// Get data source urls form the function arguments.
///
/// The returned vec is guaranteed to have all urls be of the same data
/// source type, and will contain at least one url.
pub fn urls_from_args(args: &[FuncParamValue]) -> Result<Vec<DatasourceUrl>> {
    let mut args = args.iter();
    let url_arg = match args.next() {
        Some(arg) => arg.to_owned(),
        None => {
            return Err(ExtensionError::String(
                "Expected at least one argument.".to_string(),
            ))
        }
    };

    // TODO: wtf?
    let urls: Vec<DatasourceUrl> = if url_arg.is_valid::<DatasourceUrl>() {
        vec![url_arg.try_into()?]
    } else {
        url_arg.try_into()?
    };

    if urls.is_empty() {
        return Err(ExtensionError::String(
            "Expected at least one url.".to_string(),
        ));
    }

    let first = urls.first().unwrap();
    if !urls
        .iter()
        .all(|url| url.datasource_url_type() == first.datasource_url_type())
    {
        return Err(ExtensionError::String(
            "Cannot mix different types of urls.".to_string(),
        ));
    }

    Ok(urls)
}

impl<Opts> ObjScanTableFunc<Opts> {
    /// Try to pull an identifier for credentials out of arguments.
    ///
    /// Credential identifiers are expected to be the second argument to the
    /// function.
    fn credentials_from_args(&self, args: &[FuncParamValue]) -> Result<Option<IdentValue>> {
        args.get(1)
            .map(|arg| IdentValue::try_from(arg.clone()))
            .transpose()
    }
}

/// Gets a table provider for the files at location.
///
/// If the file is detected to be local, the table provider will be wrapped in a
/// local table hint.
async fn get_table_provider(
    ctx: &dyn TableFuncContextProvider,
    ft: Arc<dyn FileFormat>,
    access: Arc<dyn ObjStoreAccess>,
    locations: impl Iterator<Item = DatasourceUrl>,
) -> Result<Arc<dyn TableProvider>> {
    let state = ctx.get_session_state();
    let prov = access
        .create_table_provider(&state, ft, locations.collect())
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))?;

    Ok(prov)
}

/// Get's an object store accessor for the provided url.
///
/// If the object store requires credentials, `creds_ident` can be provided to
/// lookup saved credentials in the catalog. Otherwise individual values (access
/// keys, etc) will be pulled out of `opts`.
fn get_store_access(
    ctx: &dyn TableFuncContextProvider,
    source_url: &DatasourceUrl,
    creds_ident: Option<&IdentValue>,
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Arc<dyn ObjStoreAccess>> {
    let access: Arc<dyn ObjStoreAccess> = match creds_ident {
        Some(ident) => {
            let creds = ctx
                .get_session_catalog()
                .resolve_credentials(ident.as_str())
                .ok_or(ExtensionError::String(format!(
                    "missing credentials object: {ident}"
                )))?;

            match source_url.datasource_url_type() {
                DatasourceUrlType::Gcs => {
                    let service_account_key = match &creds.options {
                        CredentialsOptions::Gcp(o) => o.service_account_key.to_owned(),
                        other => {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for GCS, got {}",
                                other.as_str()
                            )))
                        }
                    };

                    create_gcs_table_provider(source_url, Some(service_account_key))?
                }
                DatasourceUrlType::S3 => {
                    let (access_key_id, secret_access_key) = match &creds.options {
                        CredentialsOptions::Aws(o) => {
                            (o.access_key_id.to_owned(), o.secret_access_key.to_owned())
                        }
                        other => {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for S3, got {}",
                                other.as_str()
                            )))
                        }
                    };

                    create_s3_store_access(
                        source_url,
                        &mut opts,
                        Some(access_key_id),
                        Some(secret_access_key),
                    )?
                }
                DatasourceUrlType::Azure => {
                    let (account, access_key) = match &creds.options {
                        CredentialsOptions::Azure(azure) => {
                            (azure.account_name.to_owned(), azure.access_key.to_owned())
                        }
                        other => {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for Azure, got {}",
                                other.as_str()
                            )))
                        }
                    };

                    create_azure_store_access(source_url, Some(account), Some(access_key))?
                }
                other => {
                    return Err(ExtensionError::String(format!(
                        "Cannot get {other} datasource with credentials"
                    )))
                }
            }
        }
        None => {
            // Raw credentials or No credentials
            match source_url.datasource_url_type() {
                DatasourceUrlType::Http => create_http_store_access(source_url)?,
                DatasourceUrlType::File => create_local_store_access(ctx)?,
                DatasourceUrlType::Gcs => {
                    let service_account_key = opts
                        .remove("service_account_key")
                        .map(FuncParamValue::try_into)
                        .transpose()?;

                    create_gcs_table_provider(source_url, service_account_key)?
                }
                DatasourceUrlType::S3 => {
                    let access_key_id = opts
                        .remove("access_key_id")
                        .map(FuncParamValue::try_into)
                        .transpose()?;

                    let secret_access_key = opts
                        .remove("secret_access_key")
                        .map(FuncParamValue::try_into)
                        .transpose()?;

                    create_s3_store_access(source_url, &mut opts, access_key_id, secret_access_key)?
                }
                DatasourceUrlType::Azure => {
                    let access_key = opts
                        .remove("access_key")
                        .map(FuncParamValue::try_into)
                        .transpose()?;
                    let account = opts
                        .remove("account_name")
                        .map(FuncParamValue::try_into)
                        .transpose()?;

                    create_azure_store_access(source_url, account, access_key)?
                }
            }
        }
    };

    Ok(access)
}

fn create_local_store_access(
    ctx: &dyn TableFuncContextProvider,
) -> Result<Arc<dyn ObjStoreAccess>> {
    if ctx.get_session_vars().is_cloud_instance() {
        Err(ExtensionError::String(
            "Local file access is not supported in cloud mode".to_string(),
        ))
    } else {
        Ok(Arc::new(LocalStoreAccess))
    }
}

fn create_http_store_access(source_url: &DatasourceUrl) -> Result<Arc<dyn ObjStoreAccess>> {
    Ok(Arc::new(HttpStoreAccess {
        url: source_url
            .as_url()
            .map_err(|e| ExtensionError::Access(Box::new(e)))?,
    }))
}

fn create_gcs_table_provider(
    source_url: &DatasourceUrl,
    service_account_key: Option<String>,
) -> Result<Arc<dyn ObjStoreAccess>> {
    let bucket = source_url
        .host()
        .map(|b| b.to_owned())
        .ok_or(ExtensionError::String(
            "expected bucket name in URL".to_string(),
        ))?;

    Ok(Arc::new(GcsStoreAccess {
        bucket,
        service_account_key,
    }))
}

fn create_s3_store_access(
    source_url: &DatasourceUrl,
    opts: &mut HashMap<String, FuncParamValue>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
) -> Result<Arc<dyn ObjStoreAccess>> {
    let bucket = source_url
        .host()
        .map(|b| b.to_owned())
        .ok_or(ExtensionError::String(
            "expected bucket name in URL".to_owned(),
        ))?;

    // S3 requires a region parameter.
    const REGION_KEY: &str = "region";
    let region = opts
        .remove(REGION_KEY)
        .ok_or(ExtensionError::MissingNamedArgument(REGION_KEY))?
        .try_into()?;

    Ok(Arc::new(S3StoreAccess {
        region,
        bucket,
        access_key_id,
        secret_access_key,
    }))
}

fn create_azure_store_access(
    source_url: &DatasourceUrl,
    account: Option<String>,
    access_key: Option<String>,
) -> Result<Arc<dyn ObjStoreAccess>> {
    let account = account.ok_or(ExtensionError::MissingNamedArgument("account_name"))?;
    let access_key = access_key.ok_or(ExtensionError::MissingNamedArgument("access_key"))?;

    let mut opts = StorageOptions::default();
    opts.inner
        .insert(AzureConfigKey::AccountName.as_ref().to_owned(), account);
    opts.inner
        .insert(AzureConfigKey::AccessKey.as_ref().to_owned(), access_key);

    Ok(Arc::new(GenericStoreAccess {
        base_url: ObjectStoreUrl::try_from(source_url)?,
        storage_options: opts,
    }))
}

#[derive(Debug, Clone, Copy, Default)]
pub struct GlareDBUpload;

impl ConstBuiltinFunction for GlareDBUpload {
    const NAME: &'static str = "glaredb_upload";
    const DESCRIPTION: &'static str = "Reads a file that was uploaded to GlareDB Cloud.";
    const EXAMPLE: &'static str = "SELECT * FROM glaredb_upload('my_upload.csv')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    // signature for GlareUpload is a single filename. The filename may
    // optionally contain an extension, though it is not required. Filename
    // should not be a path.
    fn signature(&self) -> Option<Signature> {
        Some(Signature::uniform(
            1,
            vec![DataType::Utf8],
            Volatility::Stable,
        ))
    }
}

#[async_trait]
impl TableFunc for GlareDBUpload {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // Uploads can only exist remotely; this operation is not meaningful
        // when not connected to remote/hybrid.
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        if args.len() != 1 {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let session_state = ctx.get_session_state();

        // NativeTableStorage is available on Remote ctx. Use store already
        // constructed there.
        let storage = ctx
            .get_session_state()
            .config()
            .get_extension::<NativeTableStorage>()
            .ok_or_else(|| {
                ExtensionError::String(
                    format!(
                        "access unavailable, {} is not supported in local environments",
                        self.name(),
                    )
                    .to_string(),
                )
            })?;
        let store = storage.store.clone(); // Cheap to clone

        // TODO: Parse format from argument. We can start by only accepting
        //       csv, ndJSON, parquet
        // let format = match ext.as_str() {
        //     "csv" => Some(CsvFormat::default().with_schema_infer_max_rec(Some(20480))),
        //     // TODO: Add Parquet, ndJSON
        //     _ext => None,
        // }.ok_or_else(|| ExtensionError::String(format!("unsupported file extension: {fname}")))?;
        // let _format: Arc<dyn FileFormat> = Arc::new(format);
        let file_format = CsvFormat::default().with_schema_infer_max_rec(Some(20480));
        let file_format: Arc<dyn FileFormat> = Arc::new(file_format);

        // TODO: I'm unsure how this is getting applied still - is it needed
        //       after we get the obj meta still, or is the base_url and objs
        //       list good enough?
        let prefix: ObjectStorePath = format!("databases/{}/uploads/", storage.db_id()).into();
        
        // It's unfortunate that we need this and prefix...maybe there's a
        // refactor on ObjStoreTableProvider...need to look more deeply
        let base_url = format!("{}", storage.root_url);
        let base_url = ObjectStoreUrl::parse(base_url)?;

        // There should only be one object...just getting to compile and copying
        // approach using in other obj stores. Probably some cleaner syntax.
        let mut objects = store.list(Some(&prefix));
        let meta = objects
            .next()
            .await
            .ok_or_else(|| ExtensionError::String("todo".to_string()))?
            .expect("todo");
        let mut objects: Vec<ObjectMeta> = Vec::new();
        objects.push(meta);

        // Infer schema
        let arrow_schema = file_format
            .infer_schema(&session_state, &store.inner, &objects)
            .await?;

        return Ok(Arc::new(ObjStoreTableProvider::new(
            store.inner.clone(),
            arrow_schema,
            base_url,
            objects,
            file_format,
        )));
    }
}
