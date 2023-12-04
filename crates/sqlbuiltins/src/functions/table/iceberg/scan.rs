use crate::builtins::ConstBuiltinFunction;

use super::*;

/// Scan an iceberg table.
#[derive(Debug, Clone, Copy)]
pub struct IcebergScan;

impl ConstBuiltinFunction for IcebergScan {
    const NAME: &'static str = "iceberg_scan";
    const DESCRIPTION: &'static str = "Scans an iceberg table";
    const EXAMPLE: &'static str = "SELECT * FROM iceberg_scan('file:///path/to/table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for IcebergScan {
    fn runtime_preference(&self) -> RuntimePreference {
        // TODO: Detect runtime
        RuntimePreference::Remote
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        // TODO: Reduce duplication
        let (loc, opts) = table_location_and_opts(ctx, args, &mut opts)?;

        let store = storage_options_into_object_store(&loc, &opts).map_err(box_err)?;
        let table = IcebergTable::open(loc.clone(), store)
            .await
            .map_err(box_err)?;
        let reader = table.table_reader().await.map_err(box_err)?;

        Ok(reader)
    }
}
