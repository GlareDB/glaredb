//! Helpers for handling csv files.
use std::any::Any;
use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Buf, Bytes};
use datafusion::arrow::csv;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::csv::CsvFormat;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DatafusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::logical_expr::TableType;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::file_format::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig, FileStream,
};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream, Statistics,
};
use datafusion::prelude::{Expr, SessionContext};
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{GetResult, ObjectStore};

use crate::errors::Result;
use crate::TableAccessor;

/// Table provider for csv table
pub struct CsvTableProvider<T>
where
    T: TableAccessor,
{
    pub(crate) predicate_pushdown: bool,
    pub(crate) accessor: T,
    /// Schema for csv file
    pub(crate) arrow_schema: ArrowSchemaRef,
}

impl<T> CsvTableProvider<T>
where
    T: TableAccessor,
{
    pub async fn from_table_accessor(
        accessor: T,
        predicate_pushdown: bool,
    ) -> Result<CsvTableProvider<T>> {
        let store = accessor.store();
        let location = [accessor.object_meta().as_ref().clone()];

        let csv_format = CsvFormat::default();
        let session_ctx = SessionContext::new();
        let state = session_ctx.state();

        // TODO get schema another way
        let arrow_schema = csv_format.infer_schema(&state, store, &location).await?;

        Ok(CsvTableProvider {
            predicate_pushdown,
            accessor,
            arrow_schema,
        })
    }
}

#[async_trait]
impl<T> TableProvider for CsvTableProvider<T>
where
    T: TableAccessor + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        _ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        let predicate = if self.predicate_pushdown {
            filters
                .iter()
                .cloned()
                .reduce(|accum, expr| accum.and(expr))
        } else {
            None
        };

        let file = self.accessor.object_meta().as_ref().clone().into();

        let base_config = FileScanConfig {
            // `store` to `CsvExec` to use instead of the datafusion object store registry.
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_schema: self.arrow_schema.clone(),
            file_groups: vec![vec![file]],
            statistics: Statistics::default(),
            projection: projection.cloned(),
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: None,
            infinite_source: false,
        };

        let has_header = true;
        let delimiter = b',';

        let exec = CsvExec::new(
            has_header,
            delimiter,
            FileCompressionType::UNCOMPRESSED,
            self.arrow_schema.clone(),
            projection.cloned(),
            self.accessor.store().clone(),
            base_config,
        );

        Ok(Arc::new(exec))
    }
}

// /// Execution plan for scanning a CSV file
#[derive(Debug, Clone)]
pub struct CsvExec {
    store: Arc<dyn ObjectStore>,
    arrow_schema: ArrowSchemaRef,
    has_header: bool,
    delimiter: u8,
    projection: Option<Vec<usize>>,
    file_compression_type: FileCompressionType,
    base_config: FileScanConfig,
}

impl CsvExec {
    /// Create a new CSV reader execution plan provided base and specific
    /// configurations
    pub fn new(
        has_header: bool,
        delimiter: u8,
        file_compression_type: FileCompressionType,
        arrow_schema: ArrowSchemaRef,
        projection: Option<Vec<usize>>,
        store: Arc<dyn ObjectStore>,
        base_config: FileScanConfig,
    ) -> Self {
        Self {
            arrow_schema,
            has_header,
            delimiter,
            file_compression_type,
            projection,
            store,
            base_config,
        }
    }

    /// true if the first line of each file is a header
    pub fn has_header(&self) -> bool {
        self.has_header
    }

    /// A column delimiter
    pub fn delimiter(&self) -> u8 {
        self.delimiter
    }
}

impl ExecutionPlan for CsvExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> ArrowSchemaRef {
        self.arrow_schema.clone()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    /// See comments on `impl ExecutionPlan for ParquetExec`: output order can't
    /// be
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DatafusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(
            "cannot replace children for CsvExec".to_string(),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DatafusionResult<SendableRecordBatchStream> {
        let config = Arc::new(CsvConfig {
            batch_size: 8192,
            file_schema: self.arrow_schema.clone(),
            file_projection: self.projection.clone(),
            has_header: self.has_header,
            delimiter: self.delimiter,
            object_store: self.store.clone(),
        });

        let opener = CsvOpener {
            config,
            file_compression_type: self.file_compression_type.to_owned(),
        };

        let stream = FileStream::new(
            &self.base_config,
            partition,
            opener,
            &ExecutionPlanMetricsSet::new(),
        )?;
        Ok(Box::pin(stream) as SendableRecordBatchStream)
    }

    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        // TODO: need proper display for CsvExec
        tracing::warn!("No display format for CsvExec");
        write!(f, "CsvExec: file=")
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

#[derive(Debug, Clone)]
struct CsvConfig {
    batch_size: usize,
    file_schema: ArrowSchemaRef,
    file_projection: Option<Vec<usize>>,
    has_header: bool,
    delimiter: u8,
    object_store: Arc<dyn ObjectStore>,
}

impl CsvConfig {
    fn open<R: std::io::Read>(&self, reader: R, first_chunk: bool) -> csv::Reader<R> {
        let datetime_format = None;
        csv::Reader::new(
            reader,
            Arc::clone(&self.file_schema),
            self.has_header && first_chunk,
            Some(self.delimiter),
            self.batch_size,
            None,
            self.file_projection.clone(),
            datetime_format,
        )
    }
}

struct CsvOpener {
    config: Arc<CsvConfig>,
    file_compression_type: FileCompressionType,
}

impl FileOpener for CsvOpener {
    fn open(&self, file_meta: FileMeta) -> DatafusionResult<FileOpenFuture> {
        let config = self.config.clone();
        let file_compression_type = self.file_compression_type.to_owned();
        Ok(Box::pin(async move {
            match config.object_store.get(file_meta.location()).await? {
                GetResult::File(file, _) => {
                    let decoder = file_compression_type.convert_read(file)?;
                    Ok(futures::stream::iter(config.open(decoder, true)).boxed())
                }
                GetResult::Stream(s) => {
                    let mut first_chunk = true;
                    let s = s.map_err(Into::<DataFusionError>::into);
                    let decoder = file_compression_type.convert_stream(s)?;
                    Ok(newline_delimited_stream(decoder)
                        .map_ok(move |bytes| {
                            let reader = config.open(bytes.reader(), first_chunk);
                            first_chunk = false;
                            futures::stream::iter(reader)
                        })
                        .try_flatten()
                        .boxed())
                }
            }
        }))
    }
}

/// Given a [`Stream`] of [`Bytes`] returns a [`Stream`] where each
/// yielded [`Bytes`] contains a whole number of new line delimited records
/// accounting for `\` style escapes and `"` quotes
pub fn newline_delimited_stream<S>(s: S) -> impl Stream<Item = DatafusionResult<Bytes>>
where
    S: Stream<Item = DatafusionResult<Bytes>> + Unpin,
{
    let delimiter = LineDelimiter::new();

    futures::stream::unfold(
        (s, delimiter, false),
        |(mut s, mut delimiter, mut exhausted)| async move {
            loop {
                if let Some(next) = delimiter.next() {
                    return Some((Ok(next), (s, delimiter, exhausted)));
                } else if exhausted {
                    return None;
                }

                match s.next().await {
                    Some(Ok(bytes)) => delimiter.push(bytes),
                    Some(Err(e)) => return Some((Err(e), (s, delimiter, exhausted))),
                    None => {
                        exhausted = true;
                        match delimiter.finish() {
                            Ok(true) => return None,
                            Ok(false) => continue,
                            Err(e) => return Some((Err(e), (s, delimiter, exhausted))),
                        }
                    }
                }
            }
        },
    )
}

/// The ASCII encoding of `"`
const QUOTE: u8 = b'"';

/// The ASCII encoding of `\n`
const NEWLINE: u8 = b'\n';

/// The ASCII encoding of `\`
const ESCAPE: u8 = b'\\';

/// [`LineDelimiter`] is provided with a stream of [`Bytes`] and returns an
/// iterator of [`Bytes`] containing a whole number of new line delimited
/// records
#[derive(Debug, Default)]
struct LineDelimiter {
    /// Complete chunks of [`Bytes`]
    complete: VecDeque<Bytes>,
    /// Remainder bytes that form the next record
    remainder: Vec<u8>,
    /// True if the last character was the escape character
    is_escape: bool,
    /// True if currently processing a quoted string
    is_quote: bool,
}

impl LineDelimiter {
    /// Creates a new [`LineDelimiter`] with the provided delimiter
    fn new() -> Self {
        Self::default()
    }

    /// Adds the next set of [`Bytes`]
    fn push(&mut self, val: impl Into<Bytes>) {
        let val: Bytes = val.into();

        let is_escape = &mut self.is_escape;
        let is_quote = &mut self.is_quote;
        let mut record_ends = val.iter().enumerate().filter_map(|(idx, v)| {
            if *is_escape {
                *is_escape = false;
                None
            } else if *v == ESCAPE {
                *is_escape = true;
                None
            } else if *v == QUOTE {
                *is_quote = !*is_quote;
                None
            } else if *is_quote {
                None
            } else {
                (*v == NEWLINE).then_some(idx + 1)
            }
        });

        let start_offset = match self.remainder.is_empty() {
            true => 0,
            false => match record_ends.next() {
                Some(idx) => {
                    self.remainder.extend_from_slice(&val[0..idx]);
                    self.complete
                        .push_back(Bytes::from(std::mem::take(&mut self.remainder)));
                    idx
                }
                None => {
                    self.remainder.extend_from_slice(&val);
                    return;
                }
            },
        };
        let end_offset = record_ends.last().unwrap_or(start_offset);
        if start_offset != end_offset {
            self.complete.push_back(val.slice(start_offset..end_offset));
        }

        if end_offset != val.len() {
            self.remainder.extend_from_slice(&val[end_offset..])
        }
    }

    /// Marks the end of the stream, delimiting any remaining bytes
    ///
    /// Returns `true` if there is no remaining data to be read
    fn finish(&mut self) -> DatafusionResult<bool> {
        if !self.remainder.is_empty() {
            if self.is_quote {
                return Err(DataFusionError::Execution(
                    "encountered unterminated string".to_string(),
                ));
            }

            if self.is_escape {
                return Err(DataFusionError::Execution(
                    "encountered trailing escape character".to_string(),
                ));
            }

            self.complete
                .push_back(Bytes::from(std::mem::take(&mut self.remainder)))
        }
        Ok(self.complete.is_empty())
    }
}

impl Iterator for LineDelimiter {
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.complete.pop_front()
    }
}
