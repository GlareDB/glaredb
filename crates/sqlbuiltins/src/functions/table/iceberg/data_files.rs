use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Int64Builder, StringBuilder, UInt64Builder},
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

/// Scan data file metadata for the current snapshot of an iceberg table. Will
/// not attempt to read data files.
#[derive(Debug, Clone, Copy)]
pub struct IcebergDataFiles;

impl ConstBuiltinFunction for IcebergDataFiles {
    const NAME: &'static str = "iceberg_data_files";
    const DESCRIPTION: &'static str = "Scans data file metadata for an iceberg table";
    const EXAMPLE: &'static str = "SELECT * FROM iceberg_data_files('file:///path/to/table')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;
}

#[async_trait]
impl TableFunc for IcebergDataFiles {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> Result<RuntimePreference> {
        // TODO: Check URL path to detect runtime dynamically.
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

        let manifests = table
            .read_manifests()
            .await
            .map_err(ExtensionError::access)?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("manifest_index", DataType::UInt64, false),
            Field::new("manifest_content", DataType::Utf8, false),
            Field::new("snapshot_id", DataType::Int64, true),
            Field::new("sequence_number", DataType::Int64, true),
            Field::new("file_sequence_number", DataType::Int64, true),
            Field::new("file_path", DataType::Utf8, false),
            Field::new("file_format", DataType::Utf8, false),
            Field::new("record_count", DataType::Int64, false),
            Field::new("file_size_bytes", DataType::Int64, false),
        ]));

        let mut manifest_index = UInt64Builder::new();
        let mut manifest_content = StringBuilder::new();
        let mut snapshot_id = Int64Builder::new();
        let mut sequence_number = Int64Builder::new();
        let mut file_sequence_number = Int64Builder::new();
        let mut file_path = StringBuilder::new();
        let mut file_format = StringBuilder::new();
        let mut record_count = Int64Builder::new();
        let mut file_size_bytes = Int64Builder::new();

        for (idx, manifest) in manifests.into_iter().enumerate() {
            for entry in manifest.entries {
                // Manifest metadata
                manifest_index.append_value(idx as u64);
                manifest_content.append_value(manifest.metadata.content.to_string());

                // Entry data
                snapshot_id.append_option(entry.snapshot_id);
                sequence_number.append_option(entry.sequence_number);
                file_sequence_number.append_option(entry.file_sequence_number);
                file_path.append_value(&entry.data_file.file_path);
                file_format.append_value(&entry.data_file.file_format);
                record_count.append_value(entry.data_file.record_count);
                file_size_bytes.append_value(entry.data_file.file_size_in_bytes);
            }
        }

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(manifest_index.finish()),
                Arc::new(manifest_content.finish()),
                Arc::new(snapshot_id.finish()),
                Arc::new(sequence_number.finish()),
                Arc::new(file_sequence_number.finish()),
                Arc::new(file_path.finish()),
                Arc::new(file_format.finish()),
                Arc::new(record_count.finish()),
                Arc::new(file_size_bytes.finish()),
            ],
        )?;

        Ok(Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).unwrap(),
        ))
    }
}
