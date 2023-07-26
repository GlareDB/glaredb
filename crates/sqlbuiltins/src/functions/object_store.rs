use std::collections::HashMap;
use std::{sync::Arc, vec};

use async_trait::async_trait;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FromFuncParamValue, FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider,
};
use datasources::common::url::{DatasourceUrl, DatasourceUrlScheme};
use datasources::object_store::gcs::{GcsAccessor, GcsTableAccess};
use datasources::object_store::http::HttpAccessor;
use datasources::object_store::local::{LocalAccessor, LocalTableAccess};
use datasources::object_store::s3::{S3Accessor, S3TableAccess};
use datasources::object_store::{FileType, TableAccessor};
use glob::{glob_with, MatchOptions};
use metastore_client::types::options::CredentialsOptions;

pub const PARQUET_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Parquet, "parquet_scan");

pub const CSV_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Csv, "csv_scan");

pub const JSON_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Json, "ndjson_scan");

#[derive(Debug, Clone, Copy)]
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
        unimplemented!()
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

        let urls = if DatasourceUrl::is_param_valid(&url_arg) {
            let url: DatasourceUrl = url_arg.param_into()?;
            let path = url.path();
            match url.scheme() {
                DatasourceUrlScheme::File if path.contains(['*', '?', '[', ']', '!']) => {
                    // The url might be a glob pattern.
                    let paths = glob_with(
                        &path,
                        MatchOptions {
                            case_sensitive: true,
                            require_literal_separator: true,
                            require_literal_leading_dot: true,
                        },
                    )
                    .map_err(|e| ExtensionError::Access(Box::new(e)))?;

                    let mut urls = Vec::new();
                    for path in paths {
                        let path = path.map_err(|e| ExtensionError::Access(Box::new(e)))?;
                        let url = DatasourceUrl::File(path);
                        urls.push(url);
                    }
                    urls
                }
                _ => vec![url],
            }
        } else {
            url_arg.param_into::<Vec<DatasourceUrl>>()?
        };

        if urls.is_empty() {
            return Err(ExtensionError::String(
                "at least one url expected in list".to_string(),
            ));
        }

        let file_type = self.0;

        let mut urls = urls.into_iter();

        let first_url = urls.next().unwrap();
        let provider =
            create_provider_for_filetype(ctx, file_type, first_url, args.clone(), opts.clone())
                .await?;
        let source = Arc::new(DefaultTableSource::new(provider));
        let mut plan_builder = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;

        for url in urls {
            let provider =
                create_provider_for_filetype(ctx, file_type, url, args.clone(), opts.clone())
                    .await?;
            let source = Arc::new(DefaultTableSource::new(provider));
            let pb = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;
            let plan = pb.build()?;

            plan_builder = plan_builder.union(plan)?;
        }

        Ok(plan_builder.build()?)
    }
}

async fn create_provider_for_filetype(
    ctx: &dyn TableFuncContextProvider,
    file_type: FileType,
    source_url: DatasourceUrl,
    mut args: vec::IntoIter<FuncParamValue>,
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Arc<dyn TableProvider>> {
    let provider = match args.len() {
        0 => {
            // Raw credentials or No credentials
            match source_url.scheme() {
                DatasourceUrlScheme::Http => {
                    create_http_table_provider(file_type, &source_url).await?
                }
                DatasourceUrlScheme::File => {
                    create_local_table_provider(ctx, file_type, &source_url).await?
                }
                DatasourceUrlScheme::Gcs => {
                    let service_account_key = opts
                        .remove("service_account_key")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    create_gcs_table_provider(file_type, &source_url, service_account_key).await?
                }
                DatasourceUrlScheme::S3 => {
                    let access_key_id = opts
                        .remove("access_key_id")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    let secret_access_key = opts
                        .remove("secret_access_key")
                        .map(FuncParamValue::param_into)
                        .transpose()?;

                    create_s3_table_provider(
                        file_type,
                        &source_url,
                        &mut opts,
                        access_key_id,
                        secret_access_key,
                    )
                    .await?
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

            match source_url.scheme() {
                DatasourceUrlScheme::Gcs => {
                    let service_account_key = match &creds.options {
                        CredentialsOptions::Gcp(o) => o.service_account_key.to_owned(),
                        other => {
                            return Err(ExtensionError::String(format!(
                                "invalid credentials for GCS, got {}",
                                other.as_str()
                            )))
                        }
                    };

                    create_gcs_table_provider(file_type, &source_url, Some(service_account_key))
                        .await?
                }
                DatasourceUrlScheme::S3 => {
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

                    create_s3_table_provider(
                        file_type,
                        &source_url,
                        &mut opts,
                        Some(access_key_id),
                        Some(secret_access_key),
                    )
                    .await?
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
    Ok(provider)
}

async fn create_local_table_provider(
    ctx: &dyn TableFuncContextProvider,
    file_type: FileType,
    source_url: &DatasourceUrl,
) -> Result<Arc<dyn TableProvider>> {
    if *ctx.get_session_vars().is_cloud_instance.value() {
        Err(ExtensionError::String(
            "Local file access is not supported in cloud mode".to_string(),
        ))
    } else {
        let location = source_url.path().into_owned();

        LocalAccessor::new(LocalTableAccess {
            location,
            file_type: Some(file_type),
        })
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))?
        .into_table_provider(true)
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))
    }
}

async fn create_http_table_provider(
    file_type: FileType,
    source_url: &DatasourceUrl,
) -> Result<Arc<dyn TableProvider>> {
    HttpAccessor::try_new(source_url.to_string(), file_type)
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))?
        .into_table_provider(true)
        .await
        .map_err(|e| ExtensionError::Access(Box::new(e)))
}

async fn create_gcs_table_provider(
    file_type: FileType,
    source_url: &DatasourceUrl,
    service_account_key_json: Option<String>,
) -> Result<Arc<dyn TableProvider>> {
    let bucket_name = source_url
        .host()
        .map(|b| b.to_owned())
        .ok_or(ExtensionError::String(
            "expected bucket name in URL".to_string(),
        ))?;

    let location = source_url.path().into_owned();

    GcsAccessor::new(GcsTableAccess {
        bucket_name,
        service_account_key_json,
        location,
        file_type: Some(file_type),
    })
    .await
    .map_err(|e| ExtensionError::Access(Box::new(e)))?
    .into_table_provider(true)
    .await
    .map_err(|e| ExtensionError::Access(Box::new(e)))
}

async fn create_s3_table_provider(
    file_type: FileType,
    source_url: &DatasourceUrl,
    opts: &mut HashMap<String, FuncParamValue>,
    access_key_id: Option<String>,
    secret_access_key: Option<String>,
) -> Result<Arc<dyn TableProvider>> {
    let bucket_name = source_url
        .host()
        .map(|b| b.to_owned())
        .ok_or(ExtensionError::String(
            "expected bucket name in URL".to_owned(),
        ))?;

    let location = source_url.path().into_owned();

    // S3 requires a region parameter.
    const REGION_KEY: &str = "region";
    let region = opts
        .remove(REGION_KEY)
        .ok_or(ExtensionError::MissingNamedArgument(REGION_KEY))?
        .param_into()?;

    S3Accessor::new(S3TableAccess {
        bucket_name,
        location,
        file_type: Some(file_type),
        region,
        access_key_id,
        secret_access_key,
    })
    .await
    .map_err(|e| ExtensionError::Access(Box::new(e)))?
    .into_table_provider(true)
    .await
    .map_err(|e| ExtensionError::Access(Box::new(e)))
}
