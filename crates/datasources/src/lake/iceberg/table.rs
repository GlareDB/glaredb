use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::{SessionState, TaskContext};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::insert::DataSink;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    execute_stream,
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use datafusion_ext::metrics::DataSourceMetricsStreamAdapter;
use deltalake::arrow::array::{RecordBatch, UInt64Array};
use futures::{stream, StreamExt};
use iceberg::spec::{
    DataContentType,
    DataFile,
    DataFileBuilder,
    DataFileFormat,
    Manifest,
    ManifestContentType,
    ManifestEntry,
    ManifestList,
    ManifestMetadata,
    ManifestStatus,
    PartitionSpec,
    SnapshotRef,
    Struct,
    StructType,
    TableMetadata,
};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore};

use super::spec::iceberg_schema_to_arrow;
use crate::common::sink::parquet::{ParquetSink, ParquetSinkOpts};
use crate::common::url::DatasourceUrl;
use crate::common::util::COUNT_SCHEMA;
use crate::lake::iceberg::errors::{IcebergError, Result};

#[derive(Debug)]
pub struct IcebergTable {
    state: TableState,
}

impl IcebergTable {
    /// Open a table at a location using the provided object store.
    pub async fn open(
        location: DatasourceUrl,
        store: Arc<dyn ObjectStore>,
    ) -> Result<IcebergTable> {
        let state = TableState::open(location, store).await?;

        Ok(IcebergTable { state })
    }

    /// Get the table metadata.
    pub fn metadata(&self) -> &TableMetadata {
        &self.state.metadata
    }

    /// Read all manifests for the current snapshot according to the currently
    /// loaded table metadata.
    pub async fn read_manifests(&self) -> Result<Vec<Manifest>> {
        let list = self.state.read_manifest_list().await?;
        let manifests = self.state.read_manifests(&list).await?;
        Ok(manifests)
    }

    /// Get the table's arrow schema.
    pub fn table_arrow_schema(&self) -> Result<ArrowSchema> {
        self.state.table_arrow_schema()
    }

    pub async fn table_reader(&self) -> Result<Arc<dyn TableProvider>> {
        let schema = self.table_arrow_schema()?;

        Ok(Arc::new(IcebergTableReader {
            schema: Arc::new(schema),
            state: self.state.clone(),
        }))
    }
}

/// Information about the state of the table at some table version.
#[derive(Debug, Clone)]
struct TableState {
    /// The root of the table.
    location: DatasourceUrl,

    /// Store for accessing the table.
    store: Arc<dyn ObjectStore>,

    /// Loaded table metadata. Table reads will use the snapshot in this
    /// metadata.
    metadata: TableMetadata,

    /// Resolve paths relative to the table's root.
    resolver: PathResolver,
}

impl TableState {
    async fn open(location: DatasourceUrl, store: Arc<dyn ObjectStore>) -> Result<TableState> {
        // Read metadata.
        let metadata = Self::get_table_metadata(&location, &store).await?;

        let resolver = PathResolver::from_metadata(&metadata);

        Ok(TableState {
            location,
            store,
            metadata,
            resolver,
        })
    }

    async fn get_table_metadata(
        location: &DatasourceUrl,
        store: &dyn ObjectStore,
    ) -> Result<TableMetadata> {
        let path = format_object_path(location, "metadata/version-hint.text")?;

        let version_obj = match store.get(&path).await {
            Ok(get_res) => {
                let bs = get_res.bytes().await?;

                let version_contents = String::from_utf8(bs.to_vec()).map_err(|e| {
                    IcebergError::DataInvalid(format!("Expected utf-8 in version hint: {}", e))
                })?;

                // Read the first line of the `version-hint.text` file.
                let first_line = if let Some((first_line, _)) = version_contents.split_once('\n') {
                    first_line
                } else {
                    version_contents.as_str()
                };

                format_object_path(
                    location,
                    format!("metadata/v{}.metadata.json", first_line.trim()),
                )?
            }
            Err(_e) => {
                // List all the metadata files and try to get the one with the
                // latest version.

                let metadata_prefix = format_object_path(location, "metadata/")?;
                let mut metadata_objects = store.list(Some(&metadata_prefix));

                let (mut latest_v, mut latest_v_obj) = (0_u32, Option::<ObjectPath>::None);

                while let Some(obj_meta) = metadata_objects.next().await {
                    let obj_meta = obj_meta?;

                    let file_name = obj_meta.location.filename().unwrap_or_default();

                    if let Some(version_str) = file_name.strip_suffix(".metadata.json") {
                        let version_num = if let Some(version_str) = version_str.strip_prefix('v') {
                            version_str
                        } else if let Some((version_str, _uuid)) = version_str.split_once('-') {
                            // TODO: Maybe validate the "uuid". If invalid, continue.
                            version_str
                        } else {
                            continue;
                        };

                        if let Ok(version_num) = version_num.parse::<u32>() {
                            if version_num >= latest_v {
                                latest_v = version_num;
                                latest_v_obj = Some(obj_meta.location);
                            }
                        }
                    }
                }

                latest_v_obj.ok_or_else(|| {
                    IcebergError::DataInvalid(
                        "no valid iceberg table exists at the given path".to_string(),
                    )
                })?
            }
        };

        let bs = store.get(&version_obj).await?.bytes().await?;
        let metadata: TableMetadata = serde_json::from_slice(&bs).map_err(|e| {
            IcebergError::DataInvalid(format!("Failed to read table metadata: {}", e))
        })?;

        Ok(metadata)
    }

    /// Get the current snapshot from the table metadata
    fn current_snapshot(&self) -> Result<&SnapshotRef> {
        let current_snapshot = self.metadata.current_snapshot().ok_or_else(|| {
            IcebergError::DataInvalid("Missing current snapshot from metadata".to_string())
        })?;

        Ok(current_snapshot)
    }

    fn table_arrow_schema(&self) -> Result<ArrowSchema> {
        let schema = self.metadata.current_schema();
        iceberg_schema_to_arrow(schema.as_ref())
    }

    async fn read_manifests(&self, list: &ManifestList) -> Result<Vec<Manifest>> {
        let mut manifests = Vec::new();
        for ent in list.entries() {
            let manifest_path = self.resolver.relative_path(&ent.manifest_path);

            let path = format_object_path(&self.location, manifest_path)?;
            let bs = self.store.get(&path).await?.bytes().await?;

            let manifest = Manifest::parse_avro(&bs)?;
            manifests.push(manifest);
        }

        Ok(manifests)
    }

    async fn read_manifest_list(&self) -> Result<ManifestList> {
        let current_snapshot = self.current_snapshot()?;
        let manifest_list_path = self
            .resolver
            .relative_path(current_snapshot.manifest_list());

        let path = format_object_path(&self.location, manifest_list_path)?;
        let bs = self.store.get(&path).await?.bytes().await?;

        let partition_type_provider =
            |partition_spec_id: i32| -> Result<Option<StructType>, iceberg::Error> {
                self.metadata
                    .partition_spec_by_id(partition_spec_id)
                    .map(|partition_spec| {
                        partition_spec.partition_type(self.metadata.current_schema().as_ref())
                    })
                    .transpose()
            };

        let list = ManifestList::parse_with_version(
            &bs,
            self.metadata.format_version(),
            partition_type_provider,
        )?;

        Ok(list)
    }
}

/// Helper for resolving paths for files.
#[derive(Debug, Clone)]
struct PathResolver {
    /// The locations according to the tables metadata file.
    metadata_location: String,
}

impl PathResolver {
    fn from_metadata(metadata: &TableMetadata) -> PathResolver {
        PathResolver {
            metadata_location: metadata.location().to_string(),
        }
    }

    /// Get the relative path for a file according to the table's metadata.
    ///
    /// File paths in the table metadata and manifests will include the base
    /// table's location, so we need to remove that when accessing those files.
    ///
    /// E.g.
    /// manifest-list => out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro
    /// location      => out/iceberg_table
    ///
    /// This should give us:
    /// metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro
    fn relative_path<'a>(&self, path: &'a str) -> &'a str {
        // TODO: We'll probably want some better path resolution here. I'm not
        // sure what all is allowed for metadata location.

        // Remove leading "./" from metadata location
        let metadata_location = self.metadata_location.trim_start_matches("./");

        // Remove metadata location from path that was passed in.
        path.trim_start_matches(metadata_location).trim_matches('/')
    }
}

#[derive(Debug)]
pub struct IcebergTableReader {
    schema: Arc<ArrowSchema>,
    state: TableState,
}

#[async_trait]
impl TableProvider for IcebergTableReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filter_pushdown(
        &self,
        _filter: &Expr,
    ) -> DataFusionResult<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Inexact)
    }

    async fn scan(
        &self,
        ctx: &SessionState,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Create the datafusion specific url, and register the object store.
        let object_url = datasource_url_to_unique_url(&self.state.location);

        ctx.runtime_env()
            .object_store_registry
            .register_store(object_url.as_ref(), self.state.store.clone());

        // TODO: Properly prune based on partition values. This currently skips
        // any partition processing, and shoves everything into a single file
        // group when passing to the parquet exec.
        //
        // We also miss out on parallel reading by using a single file group.

        // TODO: Properly handle row-level deletes. Currently files containing
        // delete information are ignored.

        // TODO: Use provided filters to prune out partitions and/or data files
        // (since the metadata will have some info about file content).

        // TODO: Collect statistics and pass to exec.

        let manifest_list = self
            .state
            .read_manifest_list()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let manifests = self
            .state
            .read_manifests(&manifest_list)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Get only data files with "data" content.
        //
        // TODO: Handle "delete" content and also pull out partition
        // information.
        let data_files = manifests
            .iter()
            .filter(|m| matches!(m.metadata().content(), ManifestContentType::Data))
            .flat_map(|m| {
                m.entries().iter().filter_map(|ent| {
                    if ent.is_alive() {
                        Some(ent.data_file())
                    } else {
                        // Ignore deleted entries during table scans.
                        None
                    }
                })
            });

        let partitioned_files = data_files
            .map(|f| {
                let path = self.state.resolver.relative_path(f.file_path());
                let meta = ObjectMeta {
                    location: format_object_path(&self.state.location, path)?,
                    last_modified: DateTime::<Utc>::MIN_UTC, // TODO: Get the actual time.
                    size: f.file_size_in_bytes() as usize,
                    e_tag: None,
                    version: None,
                };

                Ok(PartitionedFile {
                    object_meta: meta,
                    partition_values: Vec::new(),
                    range: None,
                    extensions: None,
                })
            })
            .collect::<Result<Vec<PartitionedFile>>>()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let file_schema = self.schema();
        let statistics = Statistics::new_unknown(file_schema.as_ref());

        let conf = FileScanConfig {
            object_store_url: object_url,
            file_schema,
            projection: projection.cloned(),
            statistics,
            file_groups: vec![partitioned_files],
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: Vec::new(),
        };

        let plan = ParquetFormat::new()
            .create_physical_plan(ctx, conf, None)
            .await?;

        Ok(Arc::new(IcebergTableScan { parquet_scan: plan }))
    }

    async fn insert_into(
        &self,
        _state: &SessionState,
        _input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if overwrite {
            return Err(DataFusionError::Execution(
                "insert into iceberg table doesn't support overwrite".to_string(),
            ));
        }

        todo!()
    }
}

/// Creates a datafusion object store url from the provided data source url.
///
/// The returned object store url should be treated as a "key" for the object
/// store registry, and otherwise is semantically meaningless.
fn datasource_url_to_unique_url(url: &DatasourceUrl) -> ObjectStoreUrl {
    // Snagged this from delta-rs
    ObjectStoreUrl::parse(format!(
        "iceberg://{}{}{}",
        url.scheme(),
        url.host().unwrap_or("file"),
        url.path().replace('/', "-")
    ))
    .unwrap()
}

#[derive(Debug)]
pub struct IcebergTableScan {
    parquet_scan: Arc<dyn ExecutionPlan>,
}

impl ExecutionPlan for IcebergTableScan {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        self.parquet_scan.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.parquet_scan.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.parquet_scan.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        self.parquet_scan.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        ExecutionPlan::with_new_children(self.parquet_scan.clone(), children)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.parquet_scan.execute(partition, context)
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for IcebergTableScan {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IcebergTableScan(")?;
        self.parquet_scan.fmt_as(t, f)?;
        write!(f, ")")
    }
}

#[derive(Debug)]
pub struct IcebergTableInsert {
    state: TableState,
    input: Arc<dyn ExecutionPlan>,
    metrics: ExecutionPlanMetricsSet,
}

impl ExecutionPlan for IcebergTableInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<ArrowSchema> {
        COUNT_SCHEMA.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let input = children.pop().ok_or_else(|| {
            DataFusionError::Execution("expected 1 child for iceberg table insert".to_string())
        })?;

        if children.is_empty() {
            Ok(Arc::new(Self {
                input,
                state: self.state.clone(),
                metrics: self.metrics.clone(),
            }))
        } else {
            Err(DataFusionError::Execution(
                "expected 1 child for iceberg table insert".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(
                "expected only 1 partition for iceberg table insert".to_string(),
            ));
        }

        let path = "insert-file.parquet"; // FIXME: Generate file name.
        let object_path = format_object_path(&self.state.location, path)?;

        if let DatasourceUrl::File(fp) = &self.state.location {
            // Create the file since local object store doesn't create on its own.
            let fp = fp.join(path);
            std::fs::File::create(fp)?;
        }

        let store = self.state.store.clone();
        let sink = ParquetSink::from_obj_store(
            store.clone(),
            object_path.clone(),
            ParquetSinkOpts::default(),
        );

        let input = execute_stream(self.input.clone(), context.clone())?;

        let state = self.state.clone();
        let metadata = self.state.metadata.clone();

        let stream = stream::once(async move {
            let manifest_list = state
                .read_manifest_list()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut manifests = state
                .read_manifests(&manifest_list)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // TODO: Maybe don't use default?
            let partition_spec = metadata.default_partition_spec().ok_or_else(|| {
                DataFusionError::Execution("missing default parition spec".to_string())
            })?;

            let inserted = sink
                .write_all(input, &context)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let object_meta = store.head(&object_path).await?;

            let data_file = DataFileBuilder::default()
                .content(DataContentType::Data)
                .file_path(object_path.to_string())
                .file_format(DataFileFormat::Parquet)
                .partition(Struct::empty()) // TODO: Partition?
                .record_count(inserted)
                .file_size_in_bytes(object_meta.size as u64)
                .build()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let manifest_entry = ManifestEntry::builder()
                .status(ManifestStatus::Added)
                .data_file(data_file)
                .build();

            let current_schema = metadata.current_schema();
            let manifest_meta = ManifestMetadata::builder()
                .schema(current_schema.as_ref().clone())
                .schema_id(current_schema.schema_id())
                .format_version(metadata.format_version())
                .content(ManifestContentType::Data)
                .partition_spec(PartitionSpec::clone(partition_spec.as_ref()))
                .build();

            let manifest = Manifest::new(manifest_meta, vec![manifest_entry]);
            let manifest = manifest
                .into_avro_bytes()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let manifest_path = "insert-manifest.avro";
            let manifest_obj_path = format_object_path(&state.location, manifest_path)?;
            store.put(&manifest_obj_path, manifest.into()).await?;

            let inserted = UInt64Array::from_iter_values([inserted]);
            Ok(RecordBatch::try_new(
                COUNT_SCHEMA.clone(),
                vec![Arc::new(inserted)],
            )?)
        });

        Ok(Box::pin(DataSourceMetricsStreamAdapter::new(
            RecordBatchStreamAdapter::new(COUNT_SCHEMA.clone(), Box::pin(stream)),
            0,
            &self.metrics,
        )))
    }

    fn statistics(&self) -> DataFusionResult<Statistics> {
        Ok(Statistics::new_unknown(self.schema().as_ref()))
    }
}

impl DisplayAs for IcebergTableInsert {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "IcebergTableInsert")
    }
}

/// Formats an object path depending on if it's a url (for real object stores),
/// or if it's a local path.
fn format_object_path(
    url: &DatasourceUrl,
    path: impl AsRef<str>,
) -> Result<ObjectPath, object_store::path::Error> {
    let path = path.as_ref();

    match url {
        DatasourceUrl::Url(_) => {
            let path = format!("{}/{path}", url.path());
            ObjectPath::parse(path)
        }
        DatasourceUrl::File(root_path) => {
            let path = root_path.join(path);

            // Get absolute path without checking if the file exists or not.
            let abs_path = if path.is_absolute() {
                path
            } else {
                let cwd = std::env::current_dir().map_err(|source| {
                    object_store::path::Error::Canonicalize {
                        path: path.clone(),
                        source,
                    }
                })?;
                cwd.join(path)
            };

            ObjectPath::from_absolute_path(abs_path)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_resolve() {
        struct TestCase {
            metadata_location: &'static str,
            input: &'static str,
            expected: &'static str,
        }

        let test_cases = vec![
            // Relative table location
            TestCase {
                metadata_location: "out/iceberg_table",
                input: "out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
                expected: "metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
            },
            // Relative table location with "./"
            TestCase {
                metadata_location: "./out/iceberg_table",
                input: "out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
                expected: "metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
            },
            // Absolute table location
            TestCase {
                 metadata_location: "/Users/sean/Code/github.com/glaredb/glaredb/testdata/iceberg/tables/lineitem_versioned",
                input: "/Users/sean/Code/github.com/glaredb/glaredb/testdata/iceberg/tables/lineitem_versioned/metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
                expected: "metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
            },
            // s3 table location
            TestCase {
                 metadata_location: "s3://testdata/iceberg/tables/lineitem_versioned",
                input: "s3://testdata/iceberg/tables/lineitem_versioned/metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
                expected: "metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
            }

        ];

        for tc in test_cases {
            let resolver = PathResolver {
                metadata_location: tc.metadata_location.to_string(),
            };
            let out = resolver.relative_path(tc.input);

            assert_eq!(
                tc.expected, out,
                "metadata location: {}",
                tc.metadata_location,
            );
        }
    }
}
