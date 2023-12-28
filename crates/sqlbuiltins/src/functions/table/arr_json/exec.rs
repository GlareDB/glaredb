use std::{any::Any, io::BufReader, sync::Arc};

use async_stream::stream;
use bytes::Buf;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayDataBuilder, ArrayRef, BooleanBufferBuilder, BooleanBuilder, BufferBuilder,
            Float64Builder, GenericListArray, GenericListBuilder, Int64Array, Int64Builder,
            NullBuilder, OffsetSizeTrait, StringArray, StringBuilder, UInt64Builder,
        },
        buffer::{Buffer, NullBuffer, OffsetBuffer, ScalarBuffer},
        datatypes::{DataType, Field, SchemaRef, ToByteSlice},
        error::ArrowError,
        record_batch::RecordBatch,
    },
    datasource::{
        file_format::file_compression_type::FileCompressionType,
        physical_plan::{FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream},
    },
    error::{DataFusionError, Result},
    execution::TaskContext,
    physical_expr::{OrderingEquivalenceProperties, PhysicalSortExpr},
    physical_plan::{
        metrics::{ExecutionPlanMetricsSet, MetricsSet},
        ordering_equivalence_properties_helper, DisplayAs, DisplayFormatType, ExecutionPlan,
        Partitioning, SendableRecordBatchStream, Statistics,
    },
};
use futures::StreamExt;
use object_store::{GetResultPayload, ObjectStore};
use serde_json::Value;

// TODO add metrics and output ordering
/// Execution plan for scanning array json data source
#[derive(Debug, Clone)]
pub struct ArrayJsonExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    metrics: ExecutionPlanMetricsSet,
}

impl ArrayJsonExec {
    pub fn new(base_config: FileScanConfig, file_compression_type: FileCompressionType) -> Self {
        let (projected_schema, projected_statistics, _) = base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            file_compression_type,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    pub fn get_base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

impl DisplayAs for ArrayJsonExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "JArraysonExec: ")?;
        self.get_base_config().fmt_as(t, f)
    }
}

impl ExecutionPlan for ArrayJsonExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn unbounded_output(&self, _: &[bool]) -> Result<bool> {
        Ok(self.base_config.infinite_source)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    // TODO replace the slice with the correct implementation
    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        ordering_equivalence_properties_helper(self.schema(), &[])
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn statistics(&self) -> Statistics {
        self.projected_statistics.clone()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_size = context.session_config().batch_size();
        let (projected_schema, ..) = self.base_config.project();

        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;
        let opener = ArrayJsonOpener::new(
            batch_size,
            projected_schema,
            self.file_compression_type,
            object_store,
        );

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }
}

fn handle_nested_lists(field: &Arc<Field>, rows: &[Value]) -> Result<Arc<dyn Array>, ArrowError> {
    Ok(match field.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            // TODO instead of using "GenericListBuilder", use the type Array instead
            let mut builder: GenericListBuilder<i32, Int64Builder> =
                GenericListBuilder::with_capacity(Int64Builder::new(), rows.len());

            for row in rows.iter() {
                let val: Option<i64> = row.as_i64();
                builder.values().append_option(val);
            }

            let array = builder.finish();
            let values = array
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            Arc::new(values.clone())
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            let mut builder: GenericListBuilder<i32, Float64Builder> =
                GenericListBuilder::with_capacity(Float64Builder::new(), rows.len());

            for row in rows.iter() {
                let val: Option<f64> = row.as_f64();
                builder.values().append_option(val);
            }

            Arc::new(builder.finish())
        }
        DataType::UInt16 | DataType::UInt8 | DataType::UInt32 | DataType::UInt64 => {
            let mut builder: GenericListBuilder<i32, UInt64Builder> =
                GenericListBuilder::with_capacity(UInt64Builder::new(), rows.len());

            for row in rows.iter() {
                let val: Option<u64> = row.as_u64();
                builder.values().append_option(val);
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder: GenericListBuilder<i32, BooleanBuilder> =
                GenericListBuilder::with_capacity(BooleanBuilder::new(), rows.len());

            for row in rows.iter() {
                let val: Option<bool> = row.as_bool();
                builder.values().append_option(val);
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8 => {
            let mut builder: GenericListBuilder<i32, StringBuilder> =
                GenericListBuilder::with_capacity(
                    StringBuilder::with_capacity(rows.len(), rows.len() * 16),
                    rows.len(),
                );

            for row in rows.iter() {
                if let Some(val) = row.as_str() {
                    builder.values().append_value(val);
                } else {
                    builder.append_null();
                }
            }
            let array = builder.finish();
            let values = array
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            Arc::new(values.clone())
        }
        DataType::Null => {
            let mut builder: GenericListBuilder<i32, NullBuilder> =
                GenericListBuilder::with_capacity(NullBuilder::new(), rows.len());
            Arc::new(builder.finish())
        }
        other => {
            return Err(ArrowError::CastError(format!(
                "Failed to convert {:#?} in nested list",
                other
            )))
        }
    })
}

// TODO; use limit from passed in max_size_limit argument
fn json_values_to_record_batch(
    rows: &[Value],
    schema: SchemaRef,
    size: usize,
) -> Result<RecordBatch, ArrowError> {
    let fields = schema.fields().iter().take(size);
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(fields.len());
    for field in fields {
        let col: Arc<dyn Array> = match field.data_type() {
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                let mut arr = Int64Builder::new();
                for row in rows.iter() {
                    let val: Option<i64> = row[field.name()].as_i64();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                let mut arr = Float64Builder::new();
                for row in rows.iter() {
                    let val: Option<f64> = row[field.name()].as_f64();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::UInt16 | DataType::UInt8 | DataType::UInt32 | DataType::UInt64 => {
                let mut arr = UInt64Builder::new();
                for row in rows.iter() {
                    let val: Option<u64> = row[field.name()].as_u64();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Boolean => {
                let mut arr = BooleanBuilder::new();
                for row in rows.iter() {
                    let val: Option<bool> = row[field.name()].as_bool();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::Utf8 => {
                // Assumes an average of 16 bytes per item.
                let mut arr = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
                for row in rows.iter() {
                    let val: Option<&str> = row[field.name()].as_str();
                    arr.append_option(val);
                }
                Arc::new(arr.finish())
            }
            DataType::List(arr_field) => {
                let mut nulls = arr_field
                    .is_nullable()
                    .then(|| BooleanBufferBuilder::new(rows.len()));
                let mut offsets = vec![0];

                let mut child_data = Vec::new();

                for (idx, row) in rows.iter().enumerate() {
                    println!("index {idx}======{:#?}", child_data);

                    match (row.as_object().unwrap().get(field.name()), nulls.as_mut()) {
                        (Some(val), None) => {
                            let result =
                                handle_nested_lists(arr_field, val.as_array().unwrap())?.to_data();

                            let last_offset = *offsets.last().unwrap();
                            offsets.push(last_offset + result.len() as i32);

                            child_data.push(result);
                        }
                        (Some(val), Some(nulls)) => {
                            nulls.append(true);
                            let result =
                                handle_nested_lists(arr_field, val.as_array().unwrap())?.to_data();

                            let last_offset = *offsets.last().unwrap();
                            offsets.push(last_offset + result.len() as i32);

                            child_data.push(result);
                        }
                        (None, Some(nulls)) => nulls.append(false),
                        _ => {
                            return Err(ArrowError::JsonError(format!(
                                "Invalid key for {:#?}",
                                field.name()
                            )))
                        }
                    }
                }

                let mut offset_buffer = BufferBuilder::<i32>::new(rows.len() + 1);
                offset_buffer.append_slice(&offsets);

                let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));
                let data = ArrayDataBuilder::new(field.data_type().clone())
                    .len(rows.len())
                    .nulls(nulls)
                    .add_buffer(offset_buffer.finish())
                    .child_data(child_data);

                let arr: GenericListArray<i32> = GenericListArray::from(data.build()?);
                Arc::new(arr) as ArrayRef

                // let extracted_value = rows.iter().find_map(|row| {
                //     row.as_object()
                //         .and_then(|obj| obj.get(field.name()))
                //         .cloned()
                // });

                // if let Some(Value::Array(mut array_value)) = extracted_value {
                //     if array_value.len() < rows.len() {
                //         array_value.resize(rows.len(), Value::Null);
                //     }
                //     handle_nested_lists(arr_field, &array_value)?
                // } else {
                //     return Err(ArrowError::JsonError("failed".to_string()));
                // }
            }

            // TODO
            // 1. Nested Structs
            // 2. add timestamp and date conversions; (serde does not suppoert that though)
            other => {
                return Err(ArrowError::CastError(format!(
                    "Failed to convert {:#?}",
                    other
                )))
            }
        };

        columns.push(col);
    }
    let batch = RecordBatch::try_new(schema, columns)?;
    Ok(batch)
}

/// A [`FileOpener`] that opens an Array JSON file and yields a [`FileOpenFuture`]
struct ArrayJsonOpener {
    batch_size: usize,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
    object_store: Arc<dyn ObjectStore>,
}

impl ArrayJsonOpener {
    /// Returns an  [`ArrayJsonOpener`]
    pub fn new(
        batch_size: usize,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
        object_store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            batch_size,
            projected_schema,
            file_compression_type,
            object_store,
        }
    }
}

impl FileOpener for ArrayJsonOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let store = self.object_store.clone();
        let schema = self.projected_schema.clone();
        let batch_size = self.batch_size;
        let file_compression_type = self.file_compression_type.to_owned();
        Ok(Box::pin(async move {
            let r = store.get(file_meta.location()).await?;
            let stream = stream! {
                 match r.payload {
                GetResultPayload::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    let mut reader = BufReader::new(decoder);
                    let values: Value = serde_json::from_reader(&mut reader)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let rows = json_values_to_record_batch(values.as_array().unwrap(), schema, batch_size);
                        yield(rows);
                }
                GetResultPayload::Stream(_) => {
                      let data = r.bytes().await.map_err(|e| {
                        DataFusionError::External(Box::new(e))
                    })?;
                    let decoder = file_compression_type.convert_read(data.reader())?;
                    let mut reader = BufReader::new(decoder);
                    let values: Value = serde_json::from_reader(&mut reader)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    let rows = json_values_to_record_batch(values.as_array().unwrap(), schema, batch_size);
                    yield(rows);
                }
            };
            };
            Ok(stream.boxed())
        }))
    }
}
