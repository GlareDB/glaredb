use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Int32Builder, Int64Builder, StringBuilder},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    },
    datasource::{MemTable, TableProvider},
};
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

/// Scan snapshot information for an iceberg tables. Will not attempt to read
/// data files.
#[derive(Debug, Clone, Copy)]
pub struct IcebergSnapshots;

impl ConstBuiltinFunction for IcebergSnapshots {
    const NAME: &'static str = "iceberg_snapshots";
    const DESCRIPTION: &'static str = "Scans snapshot information for an iceberg table";
    const EXAMPLE: &'static str = "SELECT * FROM iceberg_snapshots('file:///path/to/table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for IcebergSnapshots {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        Ok(RuntimePreference::Remote)
    }

    async fn create_provider(
        &self,
        ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        mut opts: HashMap<String, FuncParamValue>,
    ) -> Result<Arc<dyn TableProvider>> {
        let (loc, opts) = table_location_and_opts(ctx, args, &mut opts)?;

        let store =
            storage_options_into_object_store(&loc, &opts).map_err(ExtensionError::access)?;
        let table = IcebergTable::open(loc, store)
            .await
            .map_err(ExtensionError::access)?;

        let snapshots = &table.metadata().snapshots;

        let schema = Arc::new(Schema::new(vec![
            Field::new("snapshot_id", DataType::Int64, false),
            Field::new("timestamp_ms", DataType::Int64, false),
            Field::new("manifest_list", DataType::Utf8, false),
            Field::new("schema_id", DataType::Int32, false),
        ]));

        let mut snapshot_id = Int64Builder::new();
        let mut timestamp_ms = Int64Builder::new();
        let mut manifest_list = StringBuilder::new();
        let mut schema_id = Int32Builder::new();

        for snapshot in snapshots {
            snapshot_id.append_value(snapshot.snapshot_id);
            timestamp_ms.append_value(snapshot.timestamp_ms);
            manifest_list.append_value(&snapshot.manifest_list);
            schema_id.append_value(snapshot.schema_id);
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(snapshot_id.finish()),
                Arc::new(timestamp_ms.finish()),
                Arc::new(manifest_list.finish()),
                Arc::new(schema_id.finish()),
            ],
        )?;

        Ok(Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
        ))
    }
}
