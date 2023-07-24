use crate::common::url::DatasourceUrl;
use crate::lake::iceberg::errors::{IcebergError, Result};
use crate::lake::iceberg::spec::{Manifest, ManifestList, TableMetadata};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::datasource::avro_to_arrow as avro;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::SessionState;
use datafusion::execution::context::TaskContext;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{TableProviderFilterPushDown, TableType};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::display::DisplayFormatType;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use datafusion::{
    arrow::datatypes::{
        DataType, Field, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef, TimeUnit,
    },
    physical_plan::memory::MemoryExec,
};
use object_store::{path::Path as ObjectPath, ObjectMeta, ObjectStore};
use std::any::Any;
use std::io::Cursor;
use std::sync::Arc;

use super::spec::{DataFile, Snapshot};

#[derive(Debug)]
pub struct IcebergTable {
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

impl IcebergTable {
    /// Open a table at a location using the provided object store.
    pub async fn open(
        location: DatasourceUrl,
        store: Arc<dyn ObjectStore>,
    ) -> Result<IcebergTable> {
        // Get table version.
        let version = {
            let path = format_object_path(&location, "metadata/version-hint.text")?;
            let path = ObjectPath::parse(path)?;
            let bs = store.get(&path).await?.bytes().await?;
            let s = String::from_utf8(bs.to_vec()).map_err(|e| {
                IcebergError::DataInvalid(format!("Expected utf-8 in version hint: {}", e))
            })?;

            let table_version = s.parse::<i32>().map_err(|e| {
                IcebergError::DataInvalid(format!("Version hint to be a number: {}", e))
            })?;

            table_version
        };

        // Read metadata.
        let metadata = {
            let path = format_object_path(&location, format!("metadata/v{version}.metadata.json"))?;
            let bs = store.get(&path).await?.bytes().await?;
            let metadata: TableMetadata = serde_json::from_slice(&bs).map_err(|e| {
                IcebergError::DataInvalid(format!("Failed to read table metadata: {}", e))
            })?;
            metadata
        };

        let resolver = PathResolver::from_metadata(&metadata);

        Ok(IcebergTable {
            location,
            store,
            metadata,
            resolver,
        })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get the current snapshot from the table metadata
    pub fn current_snapshot(&self) -> Result<&Snapshot> {
        let current_snapshot_id = self
            .metadata
            .current_snapshot_id
            .ok_or_else(|| IcebergError::DataInvalid("Missing current snapshot id".to_string()))?;

        let current_snapshot = self
            .metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == current_snapshot_id)
            .ok_or_else(|| {
                IcebergError::DataInvalid(format!(
                    "Missing snapshot for id: {}",
                    current_snapshot_id
                ))
            })?;

        Ok(current_snapshot)
    }

    /// Get the table's arrow schema.
    pub fn table_arrow_schema(&self) -> Result<ArrowSchema> {
        // v1: Read `schema`
        //
        // v2: Read `current-schema-id`, then find that correct schema in
        // `schemas`.

        if self.metadata.format_version != 2 {
            return Err(IcebergError::UnsupportedFormatVersion(
                self.metadata.format_version,
            ));
        }

        let schema = self
            .metadata
            .schemas
            .iter()
            .find(|s| s.schema_id == self.metadata.current_schema_id)
            .ok_or_else(|| {
                IcebergError::DataInvalid(format!(
                    "Missing schema for id: {}",
                    self.metadata.current_schema_id
                ))
            })?;

        schema.to_arrow_schema()
    }

    /// Read the manifests using the table's current snapshot.
    pub async fn read_manifests(&self) -> Result<Vec<Manifest>> {
        let list = self.read_manifest_list().await?;

        let mut manifests = Vec::new();
        for ent in list.entries {
            let manifest_path = self.resolver.relative_path(&ent.manifest_path);

            let path = format_object_path(&self.location, manifest_path)?;
            let bs = self.store.get(&path).await?.bytes().await?;

            let cursor = Cursor::new(bs);

            let manifest = Manifest::from_raw_avro(cursor)?;
            manifests.push(manifest);
        }

        Ok(manifests)
    }

    /// Read the manifest list using the table's current snapshot.
    async fn read_manifest_list(&self) -> Result<ManifestList> {
        let current_snapshot = self.current_snapshot()?;
        let manifest_list_path = self.resolver.relative_path(&current_snapshot.manifest_list);

        let path = format_object_path(&self.location, manifest_list_path)?;
        let bs = self.store.get(&path).await?.bytes().await?;

        let mut cursor = Cursor::new(bs);

        // HACK: Avro reader will panic since it thinks there's a null in
        // the partitions list during array validation. I'm not sure why yet
        // though.
        // TODO: Try not converting to record batch
        let schema = avro::read_avro_schema_from_reader(&mut cursor)?;
        let fields: Vec<_> = schema
            .fields
            .iter()
            .map(|f| {
                if f.name() == "partitions" {
                    let struct_field = Field::new_struct(
                        "struct",
                        vec![
                            Field::new("r508.contains_null", DataType::Boolean, false),
                            Field::new("r508.contains_nan", DataType::Boolean, true),
                            Field::new("r508.lower_bound", DataType::Binary, true),
                            Field::new("r508.upper_bound", DataType::Binary, true),
                        ],
                        true, // This is the difference. It get inferred as 'false' by the reader.
                    );
                    Field::new_list("partitions", struct_field, true)
                } else {
                    Field::new(f.name(), f.data_type().clone(), f.is_nullable())
                }
            })
            .collect();
        let schema = ArrowSchema::new(fields);

        let mut reader = avro::ReaderBuilder::new()
            .with_schema(Arc::new(schema))
            .build(cursor)?;

        let batch = reader.next().transpose()?.ok_or_else(|| {
            IcebergError::DataInvalid("No data found in manifest list".to_string())
        })?;

        let list = ManifestList::try_from_batch(batch)?;

        Ok(list)
    }

    pub async fn table_reader(&self) -> Result<Arc<dyn TableProvider>> {
        let manifests = self.read_manifests().await?;

        // TODO: Only get data files where a manifest's `content` field is
        // "data".

        let data_files: Vec<_> = manifests
            .into_iter()
            .flat_map(|m| m.entries.into_iter().map(|ent| ent.data_file))
            .collect();

        let schema = self.table_arrow_schema()?;

        Ok(Arc::new(IcebergTableReader {
            location: self.location.clone(),
            store: self.store.clone(),
            schema: Arc::new(schema),
            files: data_files,
            resolver: self.resolver.clone(),
        }))
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
            metadata_location: metadata.location.clone(),
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
    fn relative_path<'a, 'b>(&'a self, path: &'b str) -> &'b str {
        path.trim_start_matches(&self.metadata_location)
            .trim_matches('/')
    }
}

#[derive(Debug)]
pub struct IcebergTableReader {
    location: DatasourceUrl,
    store: Arc<dyn ObjectStore>,
    schema: Arc<ArrowSchema>,
    files: Vec<DataFile>,
    resolver: PathResolver,
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
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Create the datafusion specific url, and register the object store.
        let object_url = datasource_url_to_unique_url(&self.location);
        ctx.runtime_env()
            .object_store_registry
            .register_store(object_url.as_ref(), self.store.clone());

        // TODO: Partitions. Currently assuming all data files are part of the
        // same partition.

        let partitioned_files = self
            .files
            .iter()
            .map(|f| {
                let path = self.resolver.relative_path(&f.file_path);
                let meta = ObjectMeta {
                    location: format_object_path(&self.location, &path)?,
                    last_modified: DateTime::<Utc>::MIN_UTC, // TODO: Get the actual time.
                    size: f.file_size_in_bytes as usize,
                    e_tag: None,
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

        let conf = FileScanConfig {
            object_store_url: object_url,
            file_schema: self.schema(),
            projection: projection.cloned(),
            statistics: Statistics::default(),
            file_groups: vec![partitioned_files],
            limit,
            table_partition_cols: Vec::new(),
            output_ordering: Vec::new(),
            infinite_source: false,
        };

        let plan = ParquetFormat::new()
            .create_physical_plan(ctx, conf, None)
            .await?;

        Ok(Arc::new(IcebergTableScan { parquet_scan: plan }))
    }
}

/// Creates a datafusion object store url from the provided data source url.
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
        vec![self.parquet_scan.clone()]
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

    fn statistics(&self) -> Statistics {
        Statistics::default()
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
            ObjectPath::from_filesystem_path(path)
        }
    }
}
