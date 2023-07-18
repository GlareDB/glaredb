use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ListTables;

#[async_trait]
impl TableFunc for ListTables {
    fn name(&self) -> &str {
        "list_tables"
    }

    fn parameters(&self) -> &[TableFuncParameters] {
        const PARAMS: &[TableFuncParameters] = &[TableFuncParameters {
            params: &[
                TableFuncParameter {
                    name: "database",
                    typ: DataType::Utf8,
                },
                TableFuncParameter {
                    name: "schema",
                    typ: DataType::Utf8,
                },
            ],
        }];

        PARAMS
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            2 => {
                let mut args = args.into_iter();
                let database: IdentValue = args.next().unwrap().param_into()?;
                let schema_name: IdentValue = args.next().unwrap().param_into()?;

                let fields = vec![Field::new("table_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_db_lister(ctx, database.into()).await?;
                let tables_list = lister
                    .list_tables(schema_name.as_str())
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let tables_list: StringArray = tables_list.into_iter().map(Some).collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(tables_list)])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}
