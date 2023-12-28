use std::{any::Any, io::BufReader, sync::Arc};

use async_stream::stream;
use bytes::Buf;
use datafusion::{
    arrow::{
        array::{
            Array, ArrayDataBuilder, ArrayRef, BooleanBufferBuilder, BooleanBuilder, BufferBuilder,
            Float64Builder, GenericListArray, Int64Builder,
         StringBuilder, UInt64Builder,
        },
        buffer::NullBuffer,
        datatypes::{DataType, SchemaRef},
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

use super::builder::ArrayBuilderVariant;

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

                let mut child_builder = match arr_field.data_type() {
                    DataType::Int64 => {
                        ArrayBuilderVariant::Int64Builder(Int64Builder::with_capacity(rows.len()))
                    }
                    DataType::Float64 => ArrayBuilderVariant::Float64Builder(
                        Float64Builder::with_capacity(rows.len()),
                    ),
                    DataType::Utf8 => ArrayBuilderVariant::StringBuilder(
                        StringBuilder::with_capacity(rows.len(), rows.len() * 16),
                    ),
                    // Initialize other types
                    _ => {
                        return Err(ArrowError::JsonError(
                            "This type is not supported yet".to_string(),
                        ))
                    }
                };

                for row in rows.iter() {
                    match (row.as_object().unwrap().get(field.name()), nulls.as_mut()) {
                        (Some(val), None) => {
                            let value_arrays = val.as_array().unwrap();
                            for r in value_arrays.iter() {
                                child_builder.append_value(r)?;
                            }

                            let last_offset = *offsets.last().unwrap();
                            offsets.push(last_offset + value_arrays.len() as i32);
                        }
                        (Some(val), Some(nulls)) => {
                            nulls.append(true);
                            let value_arrays = val.as_array().unwrap();
                            for r in value_arrays.iter() {
                                child_builder.append_value(r)?;
                            }

                            let last_offset = *offsets.last().unwrap();
                            offsets.push(last_offset + value_arrays.len() as i32);
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

                let child_array = child_builder.finish()?;

                let mut offset_buffer = BufferBuilder::<i32>::new(rows.len() + 1);
                offset_buffer.append_slice(&offsets);

                let nulls = nulls.as_mut().map(|x| NullBuffer::new(x.finish()));
                let data = ArrayDataBuilder::new(field.data_type().clone())
                    .len(rows.len())
                    .nulls(nulls)
                    .add_buffer(offset_buffer.finish())
                    .child_data(vec![child_array.to_data().clone()]);

                let arr: GenericListArray<i32> = GenericListArray::from(data.build()?);
                Arc::new(arr) as ArrayRef
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
