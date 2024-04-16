use std::any::Any;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::sync::Arc;
use std::vec;

use async_trait::async_trait;
use datafusion::arrow::array::{
    ArrayRef,
    BinaryArray,
    Int64Array,
    RecordBatch,
    StringArray,
    TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use datafusion::common::{FileType, Statistics};
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::physical_plan::{FileOpener, FileScanConfig, FileStream};
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_expr::{EquivalenceProperties, LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, ExecutionPlan, Partitioning, PhysicalExpr};
use datafusion_ext::errors::Result;
use datafusion_ext::functions::FuncParamValue;
use futures::{StreamExt, TryStreamExt};
use object_store::{collect_bytes, GetOptions, ObjectMeta, ObjectStore};
use once_cell::sync::Lazy;

use super::object_store::{ObjScanTableFunc, OptionReader, WithCompression};

pub const READ_BLOB: ObjScanTableFunc<BlobOptionsReader> = ObjScanTableFunc {
    name: "read_blob",
    aliases: &["read_binary"],
    description: "reads from the selected source(s) to a binary blob.",
    example: "SELECT size, content, filename FROM read_blob('./README.md')",
    phantom: PhantomData,
};

#[derive(Debug, Clone, Copy)]
pub struct BlobOptionsReader;

#[derive(Debug, Clone, Copy)]
pub struct BlobFormat {
    file_compression_type: FileCompressionType,
}

impl Default for BlobFormat {
    fn default() -> Self {
        Self {
            file_compression_type: FileCompressionType::UNCOMPRESSED,
        }
    }
}

impl BlobFormat {
    /// Set a `FileCompressionType` of JSON
    /// - defaults to `FileCompressionType::UNCOMPRESSED`
    pub fn with_file_compression_type(
        mut self,
        file_compression_type: FileCompressionType,
    ) -> Self {
        self.file_compression_type = file_compression_type;
        self
    }
}

static BLOB_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("filename", DataType::Utf8, true),
        Field::new("content", DataType::Binary, true),
        Field::new("size", DataType::Int64, true),
        Field::new(
            "last_modified",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ]))
});

#[derive(Debug)]
struct ReadBlobExec {
    base_config: FileScanConfig,
    file_compression_type: FileCompressionType,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    projected_statistics: Statistics,
    metrics: ExecutionPlanMetricsSet,
}

impl ReadBlobExec {
    pub fn new(base_config: FileScanConfig, file_compression_type: FileCompressionType) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            base_config,
            file_compression_type,
            projected_schema,
            projected_output_ordering,
            projected_statistics,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

struct BlobOpener {
    object_store: Arc<dyn ObjectStore>,
    projected_schema: SchemaRef,
    file_compression_type: FileCompressionType,
}

impl BlobOpener {
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        projected_schema: SchemaRef,
        file_compression_type: FileCompressionType,
    ) -> Self {
        Self {
            object_store,
            projected_schema,
            file_compression_type,
        }
    }
}

impl FileOpener for BlobOpener {
    fn open(
        &self,
        file_meta: datafusion::datasource::physical_plan::FileMeta,
    ) -> DatafusionResult<datafusion::datasource::physical_plan::FileOpenFuture> {
        let store = self.object_store.clone();
        let schema = self.projected_schema.clone();
        let file_compression_type = self.file_compression_type;

        Ok(Box::pin(async move {
            let options = GetOptions::default();
            let result = store.get_opts(file_meta.location(), options).await?;

            // manually add columns based on the projected schema
            let mut columns = Vec::new();

            let mut size = schema
                .column_with_name("size")
                .map(|_| Arc::new(Int64Array::from(vec![result.meta.size as i64])) as ArrayRef);

            let mut last_modified = schema.column_with_name("last_modified").map(|_| {
                Arc::new(TimestampNanosecondArray::from_vec(
                    vec![result.meta.last_modified.timestamp_nanos()],
                    None,
                )) as ArrayRef
            });

            let mut filename = schema.column_with_name("filename").map(|_| {
                Arc::new(StringArray::from(vec![result.meta.location.to_string()])) as ArrayRef
            });

            let mut content = match schema.column_with_name("content") {
                Some(_) => {
                    let len = result.range.end - result.range.start;
                    match result.payload {
                        object_store::GetResultPayload::File(mut file, _) => {
                            let mut bytes = match file_meta.range {
                                None => file_compression_type.convert_read(file)?,
                                Some(_) => {
                                    file.seek(SeekFrom::Start(result.range.start as _))?;
                                    let limit = result.range.end - result.range.start;
                                    file_compression_type.convert_read(file.take(limit as u64))?
                                }
                            };
                            let mut data = Vec::new();
                            bytes.read_to_end(&mut data)?;

                            Some(Arc::new(BinaryArray::from_vec(vec![&data])) as ArrayRef)
                        }
                        object_store::GetResultPayload::Stream(s) => {
                            let s = s.map_err(DataFusionError::from);

                            let s = file_compression_type.convert_stream(s.boxed())?.fuse();
                            let bytes = collect_bytes(s, Some(len)).await?;
                            Some(Arc::new(BinaryArray::from_vec(vec![&bytes])) as ArrayRef)
                        }
                    }
                }
                None => None,
            };

            for field in schema.fields() {
                // we need to do this hacky option take because of the borrow checker.
                // these matches should only ever match once, but the borrow checker doesn't know that.
                // so we just wrap the value in an option to ensure it's only taken once.
                match field.name().as_str() {
                    "size" => {
                        if let Some(size) = size.take() {
                            columns.push(size);
                        }
                    }
                    "last_modified" => {
                        if let Some(last_modified) = last_modified.take() {
                            columns.push(last_modified);
                        }
                    }
                    "filename" => {
                        if let Some(filename) = filename.take() {
                            columns.push(filename);
                        }
                    }
                    "content" => {
                        if let Some(content) = content.take() {
                            columns.push(content);
                        }
                    }
                    _ => panic!("unexpected column name: {}", field.name()),
                }
            }

            let batch = RecordBatch::try_new(schema.clone(), columns)?;
            let iterator = vec![batch].into_iter().map(Ok);
            let stream = futures::stream::iter(iterator).boxed();
            Ok(stream)
        }))
    }
}

impl DisplayAs for ReadBlobExec {
    fn fmt_as(
        &self,
        t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ReadBlobExec: ")?;
        self.base_config.fmt_as(t, f)
    }
}

impl ExecutionPlan for ReadBlobExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Plan(
                "ReadBlobExec does not accept children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DatafusionResult<datafusion::execution::SendableRecordBatchStream> {
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let opener = BlobOpener::new(
            object_store,
            self.projected_schema.clone(),
            self.file_compression_type,
        );

        let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;

        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        EquivalenceProperties::new_with_orderings(self.schema(), &self.projected_output_ordering)
    }

    fn statistics(&self) -> DatafusionResult<Statistics> {
        Ok(self.projected_statistics.clone())
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[async_trait]
impl FileFormat for BlobFormat {
    fn as_any(&self) -> &dyn Any {
        self
    }
    async fn infer_schema(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _objects: &[ObjectMeta],
    ) -> DatafusionResult<SchemaRef> {
        Ok(BLOB_SCHEMA.clone())
    }

    async fn infer_stats(
        &self,
        _state: &SessionState,
        _store: &Arc<dyn ObjectStore>,
        _table_schema: SchemaRef,
        _object: &ObjectMeta,
    ) -> DatafusionResult<Statistics> {
        Ok(Statistics::new_unknown(BLOB_SCHEMA.as_ref()))
    }

    async fn create_physical_plan(
        &self,
        _state: &SessionState,
        conf: FileScanConfig,
        _filters: Option<&Arc<dyn PhysicalExpr>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ReadBlobExec::new(
            conf,
            self.file_compression_type,
        )))
    }

    fn file_type(&self) -> FileType {
        panic!("BlobFormat does not support file_type")
    }
}

impl OptionReader for BlobOptionsReader {
    type Format = BlobFormat;

    const OPTIONS: &'static [(&'static str, DataType)] = &[];

    fn read_options(_opts: &HashMap<String, FuncParamValue>) -> Result<Self::Format> {
        Ok(BlobFormat::default())
    }
}

impl WithCompression for BlobFormat {
    fn with_compression(self, compression: FileCompressionType) -> Result<Self> {
        Ok(self.with_file_compression_type(compression))
    }
}
