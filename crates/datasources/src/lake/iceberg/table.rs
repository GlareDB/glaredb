use std::any::Any;
use std::io::Cursor;
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
use datafusion::physical_plan::{
    DisplayAs,
    DisplayFormatType,
    ExecutionPlan,
    Partitioning,
    SendableRecordBatchStream,
    Statistics,
};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectMeta, ObjectStore};

use super::spec::{Manifest, ManifestContent, ManifestList, Snapshot, TableMetadata};
use crate::common::url::DatasourceUrl;
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
        let manifests = self.state.read_manifests().await?;
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
        // Get table version.
        // TODO: Handle not finding a version hint.
        let path = format_object_path(&location, "metadata/version-hint.text")?;
        let path = ObjectPath::parse(path)?;
        let bs = store.get(&path).await?.bytes().await?;
        let version_contents = String::from_utf8(bs.to_vec()).map_err(|e| {
            IcebergError::DataInvalid(format!("Expected utf-8 in version hint: {}", e))
        })?;
        // Read the first line of the `version-hint.text` file.
        let first_line = if let Some((first_line, _)) = version_contents.split_once('\n') {
            first_line
        } else {
            version_contents.as_str()
        };
        let version = first_line.trim();

        // Read metadata.
        let path = format_object_path(&location, format!("metadata/v{version}.metadata.json"))?;
        let bs = store.get(&path).await?.bytes().await?;
        let metadata: TableMetadata = serde_json::from_slice(&bs).map_err(|e| {
            IcebergError::DataInvalid(format!("Failed to read table metadata: {}", e))
        })?;

        let resolver = PathResolver::from_metadata(&metadata);

        Ok(TableState {
            location,
            store,
            metadata,
            resolver,
        })
    }

    /// Get the current snapshot from the table metadata
    fn current_snapshot(&self) -> Result<&Snapshot> {
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

    fn table_arrow_schema(&self) -> Result<ArrowSchema> {
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

    async fn read_manifests(&self) -> Result<Vec<Manifest>> {
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

    async fn read_manifest_list(&self) -> Result<ManifestList> {
        let current_snapshot = self.current_snapshot()?;
        let manifest_list_path = self.resolver.relative_path(&current_snapshot.manifest_list);

        let path = format_object_path(&self.location, manifest_list_path)?;
        let bs = self.store.get(&path).await?.bytes().await?;

        let cursor = Cursor::new(bs);
        let list = ManifestList::from_raw_avro(cursor)?;

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

        let manifests = self
            .state
            .read_manifests()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Get only data files with "data" content.
        //
        // TODO: Handle "delete" content and also pull out partition
        // information.
        let data_files: Vec<_> = manifests
            .into_iter()
            .filter(|m| matches!(m.metadata.content, ManifestContent::Data))
            .flat_map(|m| m.entries.into_iter().map(|ent| ent.data_file))
            .collect();

        let partitioned_files = data_files
            .iter()
            .map(|f| {
                let path = self.state.resolver.relative_path(&f.file_path);
                let meta = ObjectMeta {
                    location: format_object_path(&self.state.location, path)?,
                    last_modified: DateTime::<Utc>::MIN_UTC, // TODO: Get the actual time.
                    size: f.file_size_in_bytes as usize,
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
            infinite_source: false,
        };

        let plan = ParquetFormat::new()
            .create_physical_plan(ctx, conf, None)
            .await?;

        Ok(Arc::new(IcebergTableScan { parquet_scan: plan }))
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
