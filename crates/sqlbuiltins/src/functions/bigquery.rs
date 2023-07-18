use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ReadBigQuery;

#[async_trait]
impl TableFunc for ReadBigQuery {
    fn name(&self) -> &str {
        "read_bigquery"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "gcp_service_account_key",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "project_id",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "dataset_id",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table_id",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        _: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            4 => {
                let mut args = args.into_iter();
                let service_account: String = args.next().unwrap().param_into()?;
                let project_id: String = args.next().unwrap().param_into()?;
                let dataset_id: String = args.next().unwrap().param_into()?;
                let table_id: String = args.next().unwrap().param_into()?;

                let access = BigQueryAccessor::connect(service_account, project_id)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = access
                    .into_table_provider(
                        BigQueryTableAccess {
                            dataset_id,
                            table_id,
                        },
                        true,
                    )
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}
