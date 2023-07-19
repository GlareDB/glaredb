use std::vec;

use super::*;

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

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[
            TableFuncParameters {
                params: &[TableFuncParameter {
                    name: "url",
                    typ: DataType::Utf8,
                }],
            },
            // TableFuncParameters {
            //     params: &[TableFuncParameter {
            //         name: "urls",
            //         typ: DataType::List(Arc::new(Field::new("url", DataType::Utf8, false))),
            //     }],
            // },
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
            // TableFuncParameters {
            //     params: &[
            //         TableFuncParameter {
            //             name: "urls",
            //             typ: DataType::List(Arc::new(Field::new("url", DataType::Utf8, false))),
            //         },
            //         TableFuncParameter {
            //             name: "credentials",
            //             typ: DataType::Utf8,
            //         },
            //     ],
            // },
        ];

        PARAMS
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
            return Err(BuiltinError::InvalidNumArgs);
        }

        let mut args = args.into_iter();
        let url_arg = args.next().unwrap();

        let urls = if String::is_param_valid(&url_arg) {
            let url: String = url_arg.param_into()?;
            vec![url]
        } else {
            url_arg.param_into::<Vec<String>>()?
        };

        if urls.is_empty() {
            return Err(BuiltinError::Static("at least one url expected in list"));
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
    url_string: String,
    mut args: vec::IntoIter<FuncParamValue>,
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Arc<dyn TableProvider>> {
    let source_url = DatasourceUrl::new(&url_string)?;

    let provider = match args.len() {
        0 => {
            // Raw credentials or No credentials
            match source_url.scheme() {
                DatasourceUrlScheme::Http => {
                    create_http_table_provider(file_type, url_string).await?
                }
                DatasourceUrlScheme::File => {
                    create_local_table_provider(file_type, &source_url).await?
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
            let source_url = DatasourceUrl::new(url_string)?;
            let creds: IdentValue = args.next().unwrap().param_into()?;
            let creds = ctx
                .get_credentials_entry(creds.as_str())
                .ok_or(BuiltinError::Static("missing credentials object"))?;

            match source_url.scheme() {
                DatasourceUrlScheme::Gcs => {
                    let service_account_key = match &creds.options {
                        CredentialsOptions::Gcp(o) => o.service_account_key.to_owned(),
                        _ => return Err(BuiltinError::Static("invalid credentials for GCS")),
                    };

                    create_gcs_table_provider(file_type, &source_url, Some(service_account_key))
                        .await?
                }
                DatasourceUrlScheme::S3 => {
                    let (access_key_id, secret_access_key) = match &creds.options {
                        CredentialsOptions::Aws(o) => {
                            (o.access_key_id.to_owned(), o.secret_access_key.to_owned())
                        }
                        _ => return Err(BuiltinError::Static("invalid credentials for GCS")),
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
                _ => {
                    return Err(BuiltinError::Static(
                        "Unsupported datasource URL for given parameters",
                    ))
                }
            }
        }
        _ => return Err(BuiltinError::InvalidNumArgs),
    };
    Ok(provider)
}

async fn create_local_table_provider(
    file_type: FileType,
    source_url: &DatasourceUrl,
) -> Result<Arc<dyn TableProvider>> {
    let location = source_url.path().into_owned();

    LocalAccessor::new(LocalTableAccess {
        location,
        file_type: Some(file_type),
    })
    .await
    .map_err(|e| BuiltinError::Access(Box::new(e)))?
    .into_table_provider(true)
    .await
    .map_err(|e| BuiltinError::Access(Box::new(e)))
}

async fn create_http_table_provider(
    file_type: FileType,
    url_string: String,
) -> Result<Arc<dyn TableProvider>> {
    HttpAccessor::try_new(url_string, file_type)
        .await
        .map_err(|e| BuiltinError::Access(Box::new(e)))?
        .into_table_provider(true)
        .await
        .map_err(|e| BuiltinError::Access(Box::new(e)))
}

async fn create_gcs_table_provider(
    file_type: FileType,
    source_url: &DatasourceUrl,
    service_account_key_json: Option<String>,
) -> Result<Arc<dyn TableProvider>> {
    let bucket_name = source_url
        .host()
        .map(|b| b.to_owned())
        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

    let location = source_url.path().into_owned();

    GcsAccessor::new(GcsTableAccess {
        bucket_name,
        service_account_key_json,
        location,
        file_type: Some(file_type),
    })
    .await
    .map_err(|e| BuiltinError::Access(Box::new(e)))?
    .into_table_provider(true)
    .await
    .map_err(|e| BuiltinError::Access(Box::new(e)))
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
        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

    let location = source_url.path().into_owned();

    // S3 requires a region parameter.
    const REGION_KEY: &str = "region";
    let region = opts
        .remove(REGION_KEY)
        .ok_or(BuiltinError::MissingNamedArgument(REGION_KEY))?
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
    .map_err(|e| BuiltinError::Access(Box::new(e)))?
    .into_table_provider(true)
    .await
    .map_err(|e| BuiltinError::Access(Box::new(e)))
}
