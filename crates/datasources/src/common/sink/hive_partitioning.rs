use std::any::Any;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DfResult;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, SendableRecordBatchStream};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use tokio::task::JoinSet;

use crate::common::sink::write::demux::start_demuxer_task;

/// A data sink factory used to create a sink for a given path.
pub trait SinkProducer: std::fmt::Debug + Send + Sync {
    fn create_sink(&self, path: ObjectPath) -> Box<dyn DataSink>;
}

/// A data sink that takes a stream of record batches and writes them to a hive-partitioned
/// directory structure. Delegating creation of underlying sinks to a `SinkProducer`.
#[derive(Debug)]
pub struct HivePartitionedSinkAdapter<S: SinkProducer> {
    producer: S,
    partition_columns: Vec<String>,
    base_output_path: ObjectPath,
    file_extension: String,
    schema: Arc<Schema>,
}

impl<S: SinkProducer> HivePartitionedSinkAdapter<S> {
    pub fn new(
        producer: S,
        partition_columns: Vec<String>,
        base_output_path: ObjectPath,
        file_extension: String,
        schema: Arc<Schema>,
    ) -> Self {
        HivePartitionedSinkAdapter {
            producer,
            partition_columns,
            base_output_path,
            file_extension,
            schema,
        }
    }
}

impl<S: SinkProducer> fmt::Display for HivePartitionedSinkAdapter<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SinkPartitioner")
    }
}

impl<S: SinkProducer> DisplayAs for HivePartitionedSinkAdapter<S> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default => write!(f, "{self}"),
            DisplayFormatType::Verbose => write!(f, "{self}"),
        }
    }
}

#[async_trait]
impl<S: SinkProducer + 'static> DataSink for HivePartitionedSinkAdapter<S> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        stream: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> DfResult<u64> {
        if self.partition_columns.is_empty() {
            let sink = self.producer.create_sink(self.base_output_path.clone());
            return sink.write_all(stream, context).await;
        }

        let utf8_schema = cast_schema_to_utf8(&self.schema, &self.partition_columns)?;
        let column_types = get_columns_with_types(&utf8_schema, self.partition_columns.clone())?;

        let utf8_schema_inner = utf8_schema.clone();
        let partition_columns = self.partition_columns.clone();

        let utf8_stream = stream.map(move |batch_result| {
            if let Ok(batch) = batch_result {
                let casted_batch = cast_record_batch_to_utf8(
                    &batch,
                    &partition_columns,
                    utf8_schema_inner.clone(),
                )?;
                Ok(casted_batch)
            } else {
                batch_result
            }
        });

        let utf8_rb_stream = Box::pin(RecordBatchStreamAdapter::new(utf8_schema, utf8_stream));

        let (demux_task, mut file_stream_rx) = start_demuxer_task(
            utf8_rb_stream,
            context,
            Some(column_types),
            self.base_output_path.clone(),
            self.file_extension.clone(),
        );

        let mut sink_write_tasks: JoinSet<DfResult<usize>> = JoinSet::new();
        let writer_schema = remove_partition_columns(&self.schema, &self.partition_columns);

        while let Some((path, mut rx)) = file_stream_rx.recv().await {
            let ctx = context.clone();
            let sink = self.producer.create_sink(path);

            let stream = async_stream::stream! {
                while let Some(item) = rx.recv().await {
                    yield Ok(item);
                }
            };

            let rb_stream = Box::pin(RecordBatchStreamAdapter::new(writer_schema.clone(), stream));

            sink_write_tasks.spawn(async move {
                sink.write_all(rb_stream, &ctx)
                    .await
                    .map(|row_count| row_count as usize)
            });
        }

        let mut row_count = 0;

        while let Some(result) = sink_write_tasks.join_next().await {
            match result {
                Ok(r) => {
                    row_count += r?;
                }
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        match demux_task.await {
            Ok(r) => r?,
            Err(e) => {
                if e.is_panic() {
                    std::panic::resume_unwind(e.into_panic());
                } else {
                    unreachable!();
                }
            }
        }

        Ok(row_count as u64)
    }
}

/// Get the partition columns with their types from the schema.
pub fn get_columns_with_types(
    schema: &Schema,
    columns: Vec<String>,
) -> DfResult<Vec<(String, DataType)>> {
    columns
        .iter()
        .map(|col| {
            schema
                .field_with_name(col)
                .map(|field| (field.name().to_owned(), field.data_type().to_owned()))
                .map_err(|e| DataFusionError::External(Box::new(e)))
        })
        .collect()
}

// Keeping this somewhat conservative for now.
//
// (For more involved types like timestamps & floats
// casting these to strings which are ultimately used as
// file paths could be problematic because of
// special characters, precision loss etc).
fn supported_partition_column_type(data_type: &DataType) -> bool {
    matches!(data_type, |DataType::Boolean| DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Utf8)
}

fn cast_record_batch_to_utf8(
    batch: &datafusion::arrow::record_batch::RecordBatch,
    partition_columns: &Vec<String>,
    schema: Arc<Schema>,
) -> DfResult<datafusion::arrow::record_batch::RecordBatch> {
    let mut columns = batch.columns().to_vec();

    for column_name in partition_columns {
        let col_index = batch.schema().index_of(column_name).unwrap();
        let casted_array = cast(batch.column(col_index).as_ref(), &DataType::Utf8)?;
        columns[col_index] = casted_array;
    }

    let casted_batch = RecordBatch::try_new(schema, columns)?;

    Ok(casted_batch)
}

fn cast_schema_to_utf8(schema: &Schema, partition_columns: &[String]) -> DfResult<Arc<Schema>> {
    let mut fields = schema.fields().to_vec();

    for column_name in partition_columns.iter() {
        let idx = schema.index_of(column_name)?;

        let data_type = fields[idx].data_type();

        if data_type == &DataType::Utf8 {
            continue;
        } else if !supported_partition_column_type(data_type) {
            return Err(DataFusionError::Execution(
                format!("Partition column of type '{data_type}' is not supported").to_string(),
            ));
        }

        let casted_field = Field::new(column_name, DataType::Utf8, fields[idx].is_nullable());
        fields[idx] = Arc::new(casted_field);
    }

    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

fn remove_partition_columns(schema: &Schema, partition_columns: &[String]) -> Arc<Schema> {
    let filtered_schema = Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .filter(|f| !partition_columns.contains(f.name()))
            .map(|f| (**f).clone())
            .collect::<Vec<_>>(),
    ));

    filtered_schema
}
