use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::{
    errors::{ExtensionError, Result},
    functions::{FuncParamValue, TableFuncContextProvider},
};
use datasources::lake::{iceberg::table::IcebergTable, storage_options_into_object_store};
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use crate::functions::{
    table::{table_location_and_opts, TableFunc},
    ConstBuiltinFunction,
};

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

        let store =
            storage_options_into_object_store(&loc, &opts).map_err(ExtensionError::access)?;
        let table = IcebergTable::open(loc.clone(), store)
            .await
            .map_err(ExtensionError::access)?;
        let reader = table.table_reader().await.map_err(ExtensionError::access)?;

        Ok(reader)
    }
}
