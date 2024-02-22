use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, Signature, Volatility};
use datafusion::parquet::basic::ConvertedType;
use datafusion::parquet::file::reader::{FileReader, SerializedFileReader};
use datafusion::parquet::file::statistics::Statistics;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_ext::functions::{FuncParamValue, TableFuncContextProvider};
use datasources::common::url::DatasourceUrl;
use protogen::metastore::types::catalog::{FunctionType, RuntimePreference};

use super::TableFunc;
use crate::functions::ConstBuiltinFunction;

/// Modified implementation from datafusion-cli https://github.com/GlareDB/arrow-datafusion/blob/777235199414c7e83ffd5f5008fd2de8139a83d7/datafusion-cli/src/functions.rs#L296
/// It's slightly adapted to work with our codebase
/// Currently this only works with local files. We can eventually add support for remote files.
pub struct ParquetMetadataFunc;

#[async_trait]
impl TableFunc for ParquetMetadataFunc {
    fn detect_runtime(
        &self,
        _args: &[FuncParamValue],
        _parent: RuntimePreference,
    ) -> datafusion_ext::errors::Result<RuntimePreference> {
        Ok(RuntimePreference::Local)
    }

    async fn create_provider(
        &self,
        _ctx: &dyn TableFuncContextProvider,
        args: Vec<FuncParamValue>,
        _opts: HashMap<String, FuncParamValue>,
    ) -> datafusion_ext::errors::Result<Arc<dyn TableProvider>> {
        let filename: DatasourceUrl = match args.first() {
            Some(v) => v.clone().try_into()?,
            _ => return Err(datafusion_ext::errors::ExtensionError::InvalidNumArgs),
        };
        let filename = match filename {
            DatasourceUrl::File(path) => path,
            DatasourceUrl::Url(other) => {
                return Err(datafusion_ext::errors::ExtensionError::InvalidParamValue {
                    param: other.to_string(),
                    expected: "a local file",
                });
            }
        };

        let file = File::open(filename.clone())?;
        let reader = SerializedFileReader::new(file)
            .map_err(datafusion_ext::errors::ExtensionError::access)?;
        let metadata = reader.metadata();

        let schema = Arc::new(Schema::new(vec![
            Field::new("filename", DataType::Utf8, true),
            Field::new("row_group_id", DataType::Int64, true),
            Field::new("row_group_num_rows", DataType::Int64, true),
            Field::new("row_group_num_columns", DataType::Int64, true),
            Field::new("row_group_bytes", DataType::Int64, true),
            Field::new("column_id", DataType::Int64, true),
            Field::new("file_offset", DataType::Int64, true),
            Field::new("num_values", DataType::Int64, true),
            Field::new("path_in_schema", DataType::Utf8, true),
            Field::new("type", DataType::Utf8, true),
            Field::new("stats_min", DataType::Utf8, true),
            Field::new("stats_max", DataType::Utf8, true),
            Field::new("stats_null_count", DataType::Int64, true),
            Field::new("stats_distinct_count", DataType::Int64, true),
            Field::new("stats_min_value", DataType::Utf8, true),
            Field::new("stats_max_value", DataType::Utf8, true),
            Field::new("compression", DataType::Utf8, true),
            Field::new("encodings", DataType::Utf8, true),
            Field::new("index_page_offset", DataType::Int64, true),
            Field::new("dictionary_page_offset", DataType::Int64, true),
            Field::new("data_page_offset", DataType::Int64, true),
            Field::new("total_compressed_size", DataType::Int64, true),
            Field::new("total_uncompressed_size", DataType::Int64, true),
        ]));

        // construct recordbatch from metadata
        let mut filename_arr = vec![];
        let mut row_group_id_arr = vec![];
        let mut row_group_num_rows_arr = vec![];
        let mut row_group_num_columns_arr = vec![];
        let mut row_group_bytes_arr = vec![];
        let mut column_id_arr = vec![];
        let mut file_offset_arr = vec![];
        let mut num_values_arr = vec![];
        let mut path_in_schema_arr = vec![];
        let mut type_arr = vec![];
        let mut stats_min_arr = vec![];
        let mut stats_max_arr = vec![];
        let mut stats_null_count_arr = vec![];
        let mut stats_distinct_count_arr = vec![];
        let mut stats_min_value_arr = vec![];
        let mut stats_max_value_arr = vec![];
        let mut compression_arr = vec![];
        let mut encodings_arr = vec![];
        let mut index_page_offset_arr = vec![];
        let mut dictionary_page_offset_arr = vec![];
        let mut data_page_offset_arr = vec![];
        let mut total_compressed_size_arr = vec![];
        let mut total_uncompressed_size_arr = vec![];
        for (rg_idx, row_group) in metadata.row_groups().iter().enumerate() {
            for (col_idx, column) in row_group.columns().iter().enumerate() {
                filename_arr.push(filename.clone().to_str().unwrap().to_string());
                row_group_id_arr.push(rg_idx as i64);
                row_group_num_rows_arr.push(row_group.num_rows());
                row_group_num_columns_arr.push(row_group.num_columns() as i64);
                row_group_bytes_arr.push(row_group.total_byte_size());
                column_id_arr.push(col_idx as i64);
                file_offset_arr.push(column.file_offset());
                num_values_arr.push(column.num_values());
                path_in_schema_arr.push(column.column_path().to_string());
                type_arr.push(column.column_type().to_string());
                let converted_type = column.column_descr().converted_type();

                if let Some(s) = column.statistics() {
                    let (min_val, max_val) = if s.has_min_max_set() {
                        let (min_val, max_val) = convert_parquet_statistics(s, converted_type);
                        (Some(min_val), Some(max_val))
                    } else {
                        (None, None)
                    };
                    stats_min_arr.push(min_val.clone());
                    stats_max_arr.push(max_val.clone());
                    stats_null_count_arr.push(Some(s.null_count() as i64));
                    stats_distinct_count_arr.push(s.distinct_count().map(|c| c as i64));
                    stats_min_value_arr.push(min_val);
                    stats_max_value_arr.push(max_val);
                } else {
                    stats_min_arr.push(None);
                    stats_max_arr.push(None);
                    stats_null_count_arr.push(None);
                    stats_distinct_count_arr.push(None);
                    stats_min_value_arr.push(None);
                    stats_max_value_arr.push(None);
                };
                compression_arr.push(format!("{:?}", column.compression()));
                encodings_arr.push(format!("{:?}", column.encodings()));
                index_page_offset_arr.push(column.index_page_offset());
                dictionary_page_offset_arr.push(column.dictionary_page_offset());
                data_page_offset_arr.push(column.data_page_offset());
                total_compressed_size_arr.push(column.compressed_size());
                total_uncompressed_size_arr.push(column.uncompressed_size());
            }
        }

        let rb = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(filename_arr)),
                Arc::new(Int64Array::from(row_group_id_arr)),
                Arc::new(Int64Array::from(row_group_num_rows_arr)),
                Arc::new(Int64Array::from(row_group_num_columns_arr)),
                Arc::new(Int64Array::from(row_group_bytes_arr)),
                Arc::new(Int64Array::from(column_id_arr)),
                Arc::new(Int64Array::from(file_offset_arr)),
                Arc::new(Int64Array::from(num_values_arr)),
                Arc::new(StringArray::from(path_in_schema_arr)),
                Arc::new(StringArray::from(type_arr)),
                Arc::new(StringArray::from(stats_min_arr)),
                Arc::new(StringArray::from(stats_max_arr)),
                Arc::new(Int64Array::from(stats_null_count_arr)),
                Arc::new(Int64Array::from(stats_distinct_count_arr)),
                Arc::new(StringArray::from(stats_min_value_arr)),
                Arc::new(StringArray::from(stats_max_value_arr)),
                Arc::new(StringArray::from(compression_arr)),
                Arc::new(StringArray::from(encodings_arr)),
                Arc::new(Int64Array::from(index_page_offset_arr)),
                Arc::new(Int64Array::from(dictionary_page_offset_arr)),
                Arc::new(Int64Array::from(data_page_offset_arr)),
                Arc::new(Int64Array::from(total_compressed_size_arr)),
                Arc::new(Int64Array::from(total_uncompressed_size_arr)),
            ],
        )?;

        let parquet_metadata = ParquetMetadataTable { schema, batch: rb };
        Ok(Arc::new(parquet_metadata))
    }
}

fn convert_parquet_statistics(
    value: &Statistics,
    converted_type: ConvertedType,
) -> (String, String) {
    match (value, converted_type) {
        (Statistics::Boolean(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::Int32(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::Int64(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::Int96(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::Float(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::Double(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::ByteArray(val), ConvertedType::UTF8) => {
            let min_bytes = val.min();
            let max_bytes = val.max();
            let min = min_bytes
                .as_utf8()
                .map(|v| v.to_string())
                .unwrap_or_else(|_| min_bytes.to_string());

            let max = max_bytes
                .as_utf8()
                .map(|v| v.to_string())
                .unwrap_or_else(|_| max_bytes.to_string());
            (min, max)
        }
        (Statistics::ByteArray(val), _) => (val.min().to_string(), val.max().to_string()),
        (Statistics::FixedLenByteArray(val), ConvertedType::UTF8) => {
            let min_bytes = val.min();
            let max_bytes = val.max();
            let min = min_bytes
                .as_utf8()
                .map(|v| v.to_string())
                .unwrap_or_else(|_| min_bytes.to_string());

            let max = max_bytes
                .as_utf8()
                .map(|v| v.to_string())
                .unwrap_or_else(|_| max_bytes.to_string());
            (min, max)
        }
        (Statistics::FixedLenByteArray(val), _) => (val.min().to_string(), val.max().to_string()),
    }
}

impl ConstBuiltinFunction for ParquetMetadataFunc {
    const NAME: &'static str = "parquet_metadata";
    const DESCRIPTION: &'static str = "Gets metadata from a parquet file";
    const EXAMPLE: &'static str = "SELECT * FROM parquet_metadata('file:///path/to/file.parquet')";
    const FUNCTION_TYPE: FunctionType = FunctionType::TableReturning;

    fn signature(&self) -> Option<Signature> {
        // The signature of the function is a single argument of type Utf8
        // It's volatility is volatile as the parquet file can change at any time
        Some(Signature::exact(vec![DataType::Utf8], Volatility::Volatile))
    }
}

/// PARQUET_META table function
struct ParquetMetadataTable {
    schema: SchemaRef,
    batch: RecordBatch,
}

#[async_trait]
impl TableProvider for ParquetMetadataTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> datafusion::logical_expr::TableType {
        datafusion::logical_expr::TableType::Base
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryExec::try_new(
            &[vec![self.batch.clone()]],
            TableProvider::schema(self),
            projection.cloned(),
        )?))
    }
}
