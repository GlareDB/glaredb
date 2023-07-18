use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ReadSnowflake;

#[async_trait]
impl TableFunc for ReadSnowflake {
    fn name(&self) -> &str {
        "read_snowflake"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "account",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "username",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "password",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "database",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "warehouse",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "role",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "table",
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
            8 => {
                let mut args = args.into_iter();
                let account: String = args.next().unwrap().param_into()?;
                let username: String = args.next().unwrap().param_into()?;
                let password: String = args.next().unwrap().param_into()?;
                let database: String = args.next().unwrap().param_into()?;
                let warehouse: String = args.next().unwrap().param_into()?;
                let role: String = args.next().unwrap().param_into()?;
                let schema: String = args.next().unwrap().param_into()?;
                let table: String = args.next().unwrap().param_into()?;

                let conn_params = SnowflakeDbConnection {
                    account_name: account,
                    login_name: username,
                    password,
                    database_name: database,
                    warehouse,
                    role_name: Some(role),
                };
                let access_info = SnowflakeTableAccess {
                    schema_name: schema,
                    table_name: table,
                };
                let accessor = SnowflakeAccessor::connect(conn_params)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let prov = accessor
                    .into_table_provider(access_info, true)
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(prov))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}
