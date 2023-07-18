use super::*;

pub const PARQUET_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Parquet, "parquet_scan");

pub const CSV_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Csv, "csv_scan");

pub const JSON_SCAN: ObjScanTableFunc = ObjScanTableFunc(FileType::Json, "ndjson_scan");

#[derive(Debug, Clone, Copy)]
pub struct ObjScanTableFunc(FileType, &'static str);

impl ObjScanTableFunc {
    async fn create_provider_for_named(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let store = match args.len() {
            1 => {
                let mut args = args.into_iter();
                let url_string: String = args.next().unwrap().param_into()?;
                let source_url = DatasourceUrl::new(&url_string)?;

                match source_url.scheme() {
                    DatasourceUrlScheme::Http => HttpAccessor::try_new(url_string, self.0)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?,
                    DatasourceUrlScheme::File => {
                        let location = source_url.path().into_owned();
                        LocalAccessor::new(LocalTableAccess {
                            location,
                            file_type: Some(self.0),
                        })
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    }
                    DatasourceUrlScheme::Gcs => {
                        let service_account_key = opts
                            .remove("service_account_key")
                            .map(FuncParamValue::param_into)
                            .transpose()?;

                        let bucket_name = source_url
                            .host()
                            .map(|b| b.to_owned())
                            .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                        let location = source_url.path().into_owned();

                        GcsAccessor::new(GcsTableAccess {
                            bucket_name,
                            service_acccount_key_json: service_account_key,
                            location,
                            file_type: Some(self.0),
                        })
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
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
                            file_type: Some(self.0),
                            region,
                            access_key_id,
                            secret_access_key,
                        })
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    }
                }
            }
            2 => {
                let mut args = args.into_iter();
                let url_string: String = args.next().unwrap().param_into()?;
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

                        let bucket_name = source_url
                            .host()
                            .map(|b| b.to_owned())
                            .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                        let location = source_url.path().into_owned();

                        GcsAccessor::new(GcsTableAccess {
                            bucket_name,
                            service_acccount_key_json: Some(service_account_key),
                            location,
                            file_type: Some(self.0),
                        })
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    }
                    DatasourceUrlScheme::S3 => {
                        let (access_key_id, secret_access_key) = match &creds.options {
                            CredentialsOptions::Aws(o) => {
                                (o.access_key_id.to_owned(), o.secret_access_key.to_owned())
                            }
                            _ => return Err(BuiltinError::Static("invalid credentials for GCS")),
                        };

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
                            file_type: Some(self.0),
                            region,
                            access_key_id: Some(access_key_id),
                            secret_access_key: Some(secret_access_key),
                        })
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
                        .into_table_provider(true)
                        .await
                        .map_err(|e| BuiltinError::Access(Box::new(e)))?
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
        Ok(store)
    }

    async fn create_lp_for_named(
        &self,
        table_ref: OwnedTableReference,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<LogicalPlan> {
        let has_array = matches!(&args[0], FuncParamValue::Array(_));

        if has_array {
            unimplemented!("array of urls not supported yet for named args")
        }

        let provider = self.create_provider_for_named(ctx, args, opts).await?;
        let source = Arc::new(DefaultTableSource::new(provider));
        let pb = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;
        let plan = pb.build()?;
        Ok(plan)
    }

    async fn create_provider_for_positional(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let args = args.as_slice();
        let Self(file_type, _) = self;
        let first = args.get(0).unwrap();

        let url_string: String = first.clone().param_into()?;
        create_provider_for_filetype(ctx, *file_type, url_string, &args[1..], opts).await
    }

    async fn create_lp_for_positional(
        &self,
        table_ref: OwnedTableReference,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        opts: HashMap<String, FuncParamValue>,
    ) -> Result<LogicalPlan> {
        if !matches!(&args[0], FuncParamValue::Array(_)) {
            let provider = self.create_provider_for_positional(ctx, args, opts).await?;
            let source = Arc::new(DefaultTableSource::new(provider));
            let pb = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;
            let plan = pb.build()?;
            Ok(plan)
        } else {
            let first = &args[0];
            let urls: Vec<String> = first.clone().param_into()?;
            let urls = urls.as_slice();
            let first_url = &urls[0];
            let provider = create_provider_for_filetype(
                ctx,
                self.0,
                first_url.clone(),
                &args[1..],
                opts.clone(),
            )
            .await?;
            let source = Arc::new(DefaultTableSource::new(provider));

            let mut plan_builder = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;

            for url in &urls[1..] {
                let provider = create_provider_for_filetype(
                    ctx,
                    self.0,
                    url.clone(),
                    &args[1..],
                    opts.clone(),
                )
                .await?;
                let source = Arc::new(DefaultTableSource::new(provider));
                let pb = LogicalPlanBuilder::scan(table_ref.clone(), source, None)?;
                let plan = pb.build()?;

                plan_builder = plan_builder.union(plan)?;
            }
            Ok(plan_builder.build()?)
        }
    }
}

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
        if opts.is_empty() {
            self.create_lp_for_positional(table_ref, ctx, args, opts)
                .await
        } else {
            self.create_lp_for_named(table_ref, ctx, args, opts).await
        }
    }
}

async fn create_provider_for_filetype(
    ctx: &dyn TableFuncContextProvider,
    file_type: FileType,
    url_string: String,
    args: &[FuncParamValue],
    mut opts: HashMap<String, FuncParamValue>,
) -> Result<Arc<dyn TableProvider>> {
    let store = match args.len() {
        0 => {
            let source_url = DatasourceUrl::new(&url_string)?;

            match source_url.scheme() {
                DatasourceUrlScheme::Http => HttpAccessor::try_new(url_string, file_type)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?,
                DatasourceUrlScheme::File => {
                    let location = source_url.path().into_owned();
                    LocalAccessor::new(LocalTableAccess {
                        location,
                        file_type: Some(file_type),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                _ => todo!(),
            }
        }
        1 => {
            let source_url = DatasourceUrl::new(url_string)?;
            let creds = &args[0];

            match source_url.scheme() {
                DatasourceUrlScheme::Gcs => {
                    let service_account_key = if matches!(creds, FuncParamValue::Ident(_)) {
                        let creds: IdentValue = creds.clone().param_into()?;

                        let creds = ctx
                            .get_credentials_entry(creds.as_str())
                            .ok_or(BuiltinError::Static("missing credentials object"))?;

                        if let CredentialsOptions::Gcp(creds) = &creds.options {
                            creds.service_account_key.clone()
                        } else {
                            return Err(BuiltinError::Static("invalid credentials for GCS"));
                        }
                    } else {
                        opts.remove("service_account_key")
                            .map(FuncParamValue::param_into)
                            .unwrap_or_else(|| creds.clone().param_into())?
                    };

                    let bucket_name = source_url
                        .host()
                        .map(|b| b.to_owned())
                        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                    let location = source_url.path().into_owned();

                    GcsAccessor::new(GcsTableAccess {
                        bucket_name,
                        service_acccount_key_json: Some(service_account_key),
                        location,
                        file_type: Some(file_type),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                }
                _ => {
                    return Err(BuiltinError::Static(
                        "Unsupported datasource URL for given parameters",
                    ))
                }
            }
        }
        2 => {
            let mut args = args.iter();
            let url_string: String = args.next().unwrap().clone().param_into()?;
            let source_url = DatasourceUrl::new(url_string)?;

            let creds: String = args.next().unwrap().clone().param_into()?;
            let creds = ctx
                .get_credentials_entry(&creds)
                .ok_or(BuiltinError::Static("missing credentials object"))?;

            match source_url.scheme() {
                DatasourceUrlScheme::S3 => {
                    let (access_key_id, secret_access_key) = match &creds.options {
                        CredentialsOptions::Aws(o) => {
                            (o.access_key_id.to_owned(), o.secret_access_key.to_owned())
                        }
                        _ => return Err(BuiltinError::Static("invalid credentials for S3")),
                    };

                    let bucket_name = source_url
                        .host()
                        .map(|b| b.to_owned())
                        .ok_or(BuiltinError::Static("expected bucket name in URL"))?;

                    let location = source_url.path().into_owned();

                    // S3 requires a region parameter.
                    let region: String = args.next().unwrap().clone().param_into()?;

                    S3Accessor::new(S3TableAccess {
                        bucket_name,
                        location,
                        file_type: Some(file_type),
                        region,
                        access_key_id: Some(access_key_id),
                        secret_access_key: Some(secret_access_key),
                    })
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
                    .into_table_provider(true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?
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
    Ok(store)
}
