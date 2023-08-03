use std::collections::HashMap;
use std::{sync::Arc, vec};

use async_trait::async_trait;
use datafusion::common::OwnedTableReference;
use datafusion::datasource::file_format::file_type::FileType;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::{DefaultTableSource, TableProvider};
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder};
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{
    FromFuncParamValue, FuncParamValue, IdentValue, TableFunc, TableFuncContextProvider,
};
use datasources::common::url::DatasourceUrl;
use datasources::object_store::filetype_as_file_format_and_ext;
use metastore_client::types::options::CredentialsOptions;

use datasources::object_store::gcs::GcsProvider;
use datasources::object_store::http::{object_meta_from_head, HttpProvider};
use url::Url;

pub const PARQUET_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::PARQUET, "parquet_scan");

pub const CSV_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::CSV, "csv_scan");

pub const JSON_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::JSON, "ndjson_scan");

#[derive(Debug, Clone)]
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

        let location: &Url = config.table_paths.get(0).unwrap().as_ref();
        let scheme = location.scheme();

        Ok(match scheme {
            DatasourceUrl::HTTPS_SCHEME | DatasourceUrl::HTTP_SCHEME => {
                create_http_provider(config, state, &self.0).await?
            }
            DatasourceUrl::GS_SCHEME => {
                let service_account_key = extract_gcs_creds(ctx, args, opts)?;
                Arc::new(
                    GcsProvider::new(config, service_account_key)
                        .infer(state, &self.0)
                        .await
                        .map_err(|e| {
                            ExtensionError::String(format!("Unable to infer schema: {}", e))
                        })?,
                )
            }
            DatasourceUrl::S3_SCHEME => {
                todo!()
            }
            DatasourceUrl::FILE_SCHEME => {
                if *ctx.get_session_vars().is_cloud_instance.value() {
                    return Err(ExtensionError::String(
                        "Local file access is not supported in cloud mode".to_string(),
                    ));
                } else {
                    let prov = ListingTable::try_new(config.infer(state).await?)?;

                    Arc::new(prov)
                }
            }
            _ => Arc::new(
                ListingTable::try_new(config.infer(state).await?)
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
            if urls.is_empty() {
                return Err(ExtensionError::String(
                    "at least one url expected".to_owned(),
                ));
            }
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

async fn create_http_provider(
    mut listing_config: ListingTableConfig,
    state: &SessionState,
    ft: &FileType,
) -> Result<Arc<dyn TableProvider>> {
    let url = listing_config.table_paths.get(0).unwrap();
    let store = state.runtime_env().object_store(url)?;
    let base_url = url.to_string();
    let url: &Url = url.as_ref();
    let meta = object_meta_from_head(url).await.map_err(|e| {
        ExtensionError::String(format!(
            "Unable to get object metadata from {url}: {e}",
            url = url,
            e = e
        ))
    })?;

    let object_metas = vec![meta];
    let (file_fmt, file_ext) = filetype_as_file_format_and_ext(ft);

    let schema = file_fmt.infer_schema(state, &store, &object_metas).await?;

    let listing_options = ListingOptions::new(file_fmt).with_file_extension(file_ext);

    listing_config = listing_config.with_listing_options(listing_options);
    listing_config = listing_config.with_schema(schema);

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
