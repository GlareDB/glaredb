use super::*;

#[derive(Debug, Clone, Copy)]
pub struct ListSchemas;

#[async_trait]
impl TableFunc for ListSchemas {
    fn name(&self) -> &str {
        "list_schemas"
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        match args.len() {
            1 => {
                let mut args = args.into_iter();
                let database: IdentValue = args.next().unwrap().param_into()?;

                let fields = vec![Field::new("schema_name", DataType::Utf8, false)];
                let schema = Arc::new(Schema::new(fields));

                let lister = get_db_lister(ctx, database.into()).await?;
                let schema_list = lister
                    .list_schemas()
                    .await
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;
                let schema_list: StringArray = schema_list.into_iter().map(Some).collect();
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(schema_list)])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                let provider = MemTable::try_new(schema, vec![vec![batch]])
                    .map_err(|e| BuiltinError::Access(Box::new(e)))?;

                Ok(Arc::new(provider))
            }
            _ => Err(BuiltinError::InvalidNumArgs),
        }
    }
}
