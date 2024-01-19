use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use datafusion_ext::errors::{ExtensionError, Result};
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::lake::iceberg::table::IcebergTable;
use datasources::lake::storage_options_into_object_store;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use crate::functions::table::{table_location_and_opts, TableFunc};
use crate::functions::ConstBuiltinFunction;

/// Scan an iceberg table.
#[derive(Debug, Clone, Copy)]
pub struct IcebergScan;

impl ConstBuiltinFunction for IcebergScan {
    const DESCRIPTION: &'static str = "Scans an iceberg table";
    const EXAMPLE: &'static str = "SELECT * FROM iceberg_scan('file:///path/to/table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
    const NAME: &'static str = "iceberg_scan";
}

#[async_trait]
impl TableFunc for IcebergScan {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // TODO: Detect runtime
        Ok(RuntimePreference::Remote)
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
