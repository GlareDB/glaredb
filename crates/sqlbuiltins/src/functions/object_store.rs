use std::collections::HashMap;
use std::{sync::Arc, vec};

use async_trait::async_trait;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::{FileCompressionType, FileType};
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::object_store::ObjectStoreUrl;
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
use datasources::object_store::{ObjStoreAccess, ObjStoreAccessor};
use protogen::metastore::types::options::CredentialsOptions;

pub const PARQUET_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::PARQUET, "parquet_scan");

pub const CSV_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::CSV, "csv_scan");

pub const JSON_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::JSON, "ndjson_scan");

#[derive(Debug, Clone)]
pub struct ObjScanTableFunc(FileType, &'static str);

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
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<LogicalPlan> {
        if args.is_empty() {
            return Err(ExtensionError::InvalidNumArgs);
        }

        let mut args = args.into_iter();
        let url_arg = args.next().unwrap();

        let urls = if DatasourceUrl::is_param_valid(&url_arg) {
            vec![url_arg.param_into()?]
        } else {
            url_arg.param_into::<Vec<DatasourceUrl>>()?
        };

        if urls.is_empty() {
            return Err(ExtensionError::String(
                "at least one url expected".to_owned(),
            ));
        }

        let file_compression = match opts.remove("compression") {
            Some(cmp) => {
                let cmp: String = cmp.param_into()?;
                cmp.parse()?
            }
            None => {
                let path = urls
                    .first()
                    .ok_or_else(|| ExtensionError::String("at least one url expected".to_string()))?
                    .path();
                let path = std::path::Path::new(path.as_ref());
                path.extension()
                    .and_then(|ext| ext.to_string_lossy().as_ref().parse().ok())
                    .unwrap_or(FileCompressionType::UNCOMPRESSED)
            }
        };

        let Self(ft, _) = self;
        let ft: Arc<dyn FileFormat> = match ft {
            FileType::CSV => {
                Arc::new(CsvFormat::default().with_file_compression_type(file_compression))
            }
            FileType::PARQUET => Arc::new(ParquetFormat::default()),
            FileType::JSON => {
                Arc::new(JsonFormat::default().with_file_compression_type(file_compression))
            }
            ft => {
                return Err(ExtensionError::String(format!(
                    "Unsuppored file type: {ft:?}"
                )))
            }
        };

        // Optimize creating a table provider for objects by clubbing the same
        // store together.
        let mut fn_registry: HashMap<
            ObjectStoreUrl,
            (Arc<dyn ObjStoreAccess>, Vec<DatasourceUrl>),
        > = HashMap::new();
        for source_url in urls {
            let access = get_store_access(ctx, &source_url, args.clone(), opts.clone()).await?;
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
        let mut fn_registry = fn_registry.into_values();

        let (access, locations) = fn_registry.next().ok_or(ExtensionError::String(
            "internal: no urls found, at least one expected".to_string(),
        ))?;
        let first_provider =
            get_table_provider(ctx, ft.clone(), access, locations.into_iter()).await?;
        let source = Arc::new(DefaultTableSource::new(first_provider));
        let mut plan_builder = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;

        for (access, locations) in fn_registry {
            let provider =
                get_table_provider(ctx, ft.clone(), access, locations.into_iter()).await?;
            let source = Arc::new(DefaultTableSource::new(provider));
            let plan = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?.build()?;

            plan_builder = plan_builder.union(plan)?;
        }

        let plan = plan_builder.build()?;
        Ok(plan)
    }
}

async fn get_table_provider(
    ctx: &dyn TableFuncContextProvider,
    ft: Arc<dyn FileFormat>,
    access: Arc<dyn ObjStoreAccess>,
    locations: impl Iterator<Item = DatasourceUrl>,
) -> Result<Arc<dyn TableProvider>> {
    let state = ctx.get_session_state();
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
            state,
            ft,
            objects.into_iter().flatten().collect(),
            /* predicate_pushdown = */ true,
        )
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))
}

async fn get_store_access(
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
            let creds =
                ctx.get_credentials_entry(creds.as_str())
                    .await
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
        .param_into()?;

    Ok(Arc::new(S3StoreAccess {
        region,
        bucket,
        access_key_id,
        secret_access_key,
    }))
}
