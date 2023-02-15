//! Helpers for handling csv files.
use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::csv::reader::infer_reader_schema;
use datafusion::arrow::csv::ReaderBuilder;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::TableProvider;
use datafusion::error::Result as DatafusionResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::TableType;
use datafusion::physical_plan::file_format::{CsvExec, FileScanConfig};
use datafusion::physical_plan::{ExecutionPlan, Statistics};
use datafusion::prelude::Expr;
use object_store::{ObjectMeta, ObjectStore};

use crate::errors::Result;
use crate::TableAccessor;

/// Custom `CsvFileReaderFactory` to provide a custom datasource in
/// `CsvExec` instead of datafusion Object Store registry
#[derive(Debug)]
pub struct SimpleCsvFileReaderFactory {
    /// Object Store to read from
    store: Arc<dyn ObjectStore>,
}

impl SimpleCsvFileReaderFactory {
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self { store }
    }
}

/// A csv file reader using the `AsyncFileReader` interface.
#[derive(Debug)]
pub struct CsvObjectReader {
    pub store: Arc<dyn ObjectStore>,
    pub meta: Arc<ObjectMeta>,
    pub meta_size_hint: Option<usize>,
}

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
        let reader = CsvObjectReader {
            store: accessor.store().clone(),
            meta: accessor.object_meta().clone(),
            meta_size_hint: None,
        };

        let r = accessor
            .store()
            .get(&accessor.object_meta().location)
            .await?
            .bytes()
            .await?;
        // let reader = ReaderBuilder::new().build(r.into())?;
        //
        let file = std::path::PathBuf::from("/Users/rustomms/Developer/glaredb/testdata/sqllogictests_datasources_common/data/bikeshare_stations.csv");
        let reader = std::fs::File::open(file).unwrap();

        let has_header = true;
        let delimiter = ',' as u8;
        let max_read_records: Option<usize> = Some(1000);

        let (arrow_schema, _) =
            // infer_reader_schema(r.into(), delimiter, max_read_records, has_header)?;
        infer_reader_schema(reader, delimiter, max_read_records, has_header)?;

        Ok(CsvTableProvider {
            predicate_pushdown,
            accessor,
            arrow_schema: arrow_schema.into(),
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
            // `object_store_url` will be ignored as we are providing a
            // `SimpleCsvFileReaderFactory` to `CsvExec` to use instead of the
            // datafusion object store registry.
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
        let delimiter = ',' as u8;
        let exec = CsvExec::new(
            base_config,
            has_header,
            delimiter,
            FileCompressionType::UNCOMPRESSED,
        );

        // tracing::warn!("Unimplemented");
        // return Err(crate::errors::ObjectStoreSourceError::Unimplemented);
        // .with_csv_file_reader_factory(factory)
        // .with_pushdown_filters(self.predicate_pushdown);

        Ok(Arc::new(exec))
    }
}

// /// Execution plan for scanning a CSV file
// #[derive(Debug, Clone)]
// pub struct CsvExec {
// base_config: FileScanConfig,
// projected_statistics: Statistics,
// projected_schema: SchemaRef,
// has_header: bool,
// delimiter: u8,
// /// Execution metrics
// metrics: ExecutionPlanMetricsSet,
// file_compression_type: FileCompressionType,
// }
//
// impl CsvExec {
// /// Create a new CSV reader execution plan provided base and specific configurations
// pub fn new(
// base_config: FileScanConfig,
// has_header: bool,
// delimiter: u8,
// file_compression_type: FileCompressionType,
// ) -> Self {
// let (projected_schema, projected_statistics) = base_config.project();
//
// Self {
// base_config,
// projected_schema,
// projected_statistics,
// has_header,
// delimiter,
// metrics: ExecutionPlanMetricsSet::new(),
// file_compression_type,
// }
// }
//
// /// Ref to the base configs
// pub fn base_config(&self) -> &FileScanConfig {
// &self.base_config
// }
// /// true if the first line of each file is a header
// pub fn has_header(&self) -> bool {
// self.has_header
// }
// /// A column delimiter
// pub fn delimiter(&self) -> u8 {
// self.delimiter
// }
// }
//
// impl ExecutionPlan for CsvExec {
// /// Return a reference to Any that can be used for downcasting
// fn as_any(&self) -> &dyn Any {
// self
// }
//
// /// Get the schema for this execution plan
// fn schema(&self) -> SchemaRef {
// self.projected_schema.clone()
// }
//
// /// Get the output partitioning of this plan
// fn output_partitioning(&self) -> Partitioning {
// Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
// }
//
// fn unbounded_output(&self, _: &[bool]) -> Result<bool> {
// Ok(self.base_config().infinite_source)
// }
//
// /// See comments on `impl ExecutionPlan for ParquetExec`: output order can't be
// fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
// get_output_ordering(&self.base_config)
// }
//
// fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
// // this is a leaf node and has no children
// vec![]
// }
//
// fn with_new_children(
// self: Arc<Self>,
// _: Vec<Arc<dyn ExecutionPlan>>,
// ) -> Result<Arc<dyn ExecutionPlan>> {
// Ok(self)
// }
//
// fn execute(
// &self,
// partition: usize,
// context: Arc<TaskContext>,
// ) -> Result<SendableRecordBatchStream> {
// let object_store = context
// .runtime_env()
// .object_store(&self.base_config.object_store_url)?;
//
// let config = Arc::new(CsvConfig {
// batch_size: context.session_config().batch_size(),
// file_schema: Arc::clone(&self.base_config.file_schema),
// file_projection: self.base_config.file_column_projection_indices(),
// has_header: self.has_header,
// delimiter: self.delimiter,
// object_store,
// });
//
// let opener = CsvOpener {
// config,
// file_compression_type: self.file_compression_type.to_owned(),
// };
// let stream = FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
// Ok(Box::pin(stream) as SendableRecordBatchStream)
// }
//
// fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
// match t {
// DisplayFormatType::Default => {
// write!(
// f,
// "CsvExec: files={}, has_header={}, limit={:?}, projection={}",
// super::FileGroupsDisplay(&self.base_config.file_groups),
// self.has_header,
// self.base_config.limit,
// super::ProjectSchemaDisplay(&self.projected_schema),
// )
// }
// }
// }
//
// fn statistics(&self) -> Statistics {
// self.projected_statistics.clone()
// }
//
// fn metrics(&self) -> Option<MetricsSet> {
// Some(self.metrics.clone_inner())
// }
// }
