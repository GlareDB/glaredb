use std::collections::HashMap;
use std::{sync::Arc, vec};

use async_trait::async_trait;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::file_type::FileType as DataFusionFileType;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat as DataFusionFileFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FromFuncParamValue, FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider,
};
use datasources::common::url::{DatasourceUrl, DatasourceUrlType};
use datasources::object_store::gcs::GcsStoreAccess;
use datasources::object_store::http::HttpStoreAccess;
use datasources::object_store::local::LocalStoreAccess;
use datasources::object_store::s3::S3StoreAccess;
use datasources::object_store::{FileType, FileTypeAccess, ObjStoreAccess, ObjStoreAccessor};
use metastore_client::types::options::CredentialsOptions;

use datasources::object_store::gcs::GcsProvider;
use datasources::object_store::http::{object_meta_from_head, HttpProvider};

pub const PARQUET_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Parquet, "parquet_scan");

pub const CSV_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Csv, "csv_scan");

pub const JSON_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Json, "ndjson_scan");

#[derive(Debug, Clone, Copy)]
pub struct ObjScanTableFunc(FileType, &'static str);

impl ObjScanTableFunc {
    async fn create_provider_from_config(
        &self,
        config: ListingTableConfig,
        ctx: &dyn TableFuncContextProvider,
        args: vec::IntoIter<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let state = ctx.get_session_state();

        let location = config.table_paths.get(0).unwrap().as_str();
        let scheme = DatasourceUrl::try_new(location)
            .unwrap()
            .datasource_url_type();

        Ok(match scheme {
            DatasourceUrlType::Http => create_http_provider(config, state).await?,
            DatasourceUrlType::Gcs => {
                let service_account_key = extract_gcs_creds(ctx, args, opts)?;

                Arc::new(
                    GcsProvider::new(config, service_account_key)
                        .infer(state)
                        .await
                        .map_err(|e| {
                            ExtensionError::String(format!("Unable to infer schema: {}", e))
                        })?,
                )
            }
            _ => Arc::new(
                ListingTable::try_new(config.infer_schema(state).await.unwrap())
                    .ok()
                    .unwrap(),
            ),
        })
    }
}

#[async_trait]
impl TableFunc for ObjScanTableFunc {
    fn name(&self) -> &str {
        let Self(_, name) = self;
        name
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        _: Vec<FuncParamValue>,
        _: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        unimplemented!();
    }

    async fn create_logical_plan(
        &self,
        table_ref: OwnedTableReference,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<LogicalPlan> {
        if args.is_empty() {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let mut args = args.into_iter();
        let url_arg = args.next().unwrap();
        let config = if ListingTableUrl::is_param_valid(&url_arg) {
            let url = url_arg.param_into()?;
            ListingTableConfig::new(url)
        } else {
            let urls = url_arg.param_into::<Vec<ListingTableUrl>>()?;
            ListingTableConfig::new_with_multi_paths(urls)
        };

        let provider = self
            .create_provider_from_config(config, ctx, args, opts)
            .await?;

        let source = Arc::new(DefaultTableSource::new(provider));
        let plan_builder = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;

        Ok(plan_builder.build()?)
    }
}

async fn get_table_provider(
    ft: Arc<dyn FileTypeAccess>,
    access: Arc<dyn ObjStoreAccess>,
    locations: impl Iterator<Item = DatasourceUrl>,
) -> Result<Arc<dyn TableProvider>> {
    let accessor =
        ObjStoreAccessor::new(access).map_err(|e| ExtensionError::Access(Box::new(e)))?;

    let mut objects = Vec::new();
    for loc in locations {
        let list = accessor
            .list_globbed(loc.path())
            .await
            .map_err(|e| ExtensionError::Access(Box::new(e)))?;
        objects.push(list);
    }

    accessor
        .into_table_provider(
            ft,
            objects.into_iter().flatten().collect(),
            /* predicate_pushdown */ true,
        )
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))
}

fn get_store_access(
    ctx: &dyn TableFuncContextProvider,
    source_url: &DatasourceUrl,
    mut args: vec::IntoIter<FuncParamValue>,
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Arc<dyn ObjStoreAccess>> {
    let access: Arc<dyn ObjStoreAccess> = match args.len() {
        0 => {
            // Raw credentials or No credentials
            match source_url.datasource_url_type() {
                DatasourceUrlType::Http => create_http_store_access(source_url)?,
                DatasourceUrlType::File => create_local_store_access(ctx)?,
                DatasourceUrlType::Gcs => {
                    let service_account_key = opts
                        .remove("service_account_key")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    create_gcs_table_provider(source_url, service_account_key)?
                }
                DatasourceUrlType::S3 => {
                    let access_key_id = opts
                        .remove("access_key_id")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    let secret_access_key = opts
                        .remove("secret_access_key")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    create_s3_store_access(source_url, &mut opts, access_key_id, secret_access_key)?
                }
            }
        }
        1 => {
            // Credentials object
            let creds: IdentValue = args.next().unwrap().param_into()?;
            let creds = ctx
                .get_credentials_entry(creds.as_str())
                .ok_or(ExtensionError::String(format!(
                    "missing credentials object: {creds}"
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
                other => {
                    return Err(ExtensionError::String(format!(
                        "Cannot get {other} datasource with credentials"
                    )))
                }
            }
        }
        _ => return Err(ExtensionError::InvalidNumArgs),
    };

    Ok(access)
}

fn create_local_store_access(
    ctx: &dyn TableFuncContextProvider,
) -> Result<Arc<dyn ObjStoreAccess>> {
    if *ctx.get_session_vars().is_cloud_instance.value() {
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
        .param_into()?;

    Ok(Arc::new(S3StoreAccess {
        region,
        bucket,
        access_key_id,
        secret_access_key,
    }))
}

async fn create_http_provider(
    listing_config: ListingTableConfig,
    state: &SessionState,
) -> Result<Arc<dyn TableProvider>> {
    let url = listing_config.table_paths.get(0).unwrap();
    let store = state.runtime_env().object_store(url)?;
    let base_url = url.to_string();
    let url = url::Url::parse(&url.to_string()).unwrap();
    let meta = object_meta_from_head(&url).await.unwrap();
    let object_metas = vec![meta];

    let file_type = listing_config.options.clone().unwrap().format;
    let schema = file_type
        .infer_schema(state, &store, &object_metas)
        .await
        .unwrap();

    let listing_config = listing_config.with_schema(schema);

    let prov = HttpProvider {
        base_url,
        config: listing_config,
        paths: object_metas,
    };

    Ok(Arc::new(prov))
}

fn extract_gcs_creds(
    ctx: &dyn TableFuncContextProvider,
    mut args: vec::IntoIter<FuncParamValue>,
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Option<String>> {
    Ok(match args.len() {
        0 => opts
            .remove("service_account_key")
            .map(FuncParamValue::param_into)
            .transpose()?,
        1 => {
            let creds: IdentValue = args.next().unwrap().param_into()?;
            let creds = ctx
                .get_credentials_entry(creds.as_str())
                .ok_or(ExtensionError::String(format!(
                    "missing credentials object: {creds}"
                )))?;
            Some(match &creds.options {
                CredentialsOptions::Gcp(o) => o.service_account_key.to_owned(),
                other => {
                    return Err(ExtensionError::String(format!(
                        "invalid credentials for GCS, got {}",
                        other.as_str()
                    )))
                }
            })
        }
        _ => None,
    })
}
