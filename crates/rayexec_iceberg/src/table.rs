use core::str;
use std::collections::VecDeque;
use std::io::Cursor;
use std::sync::Arc;

use futures::StreamExt;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::arrays::batch::Batch2;
use rayexec_execution::arrays::field::Schema;
use rayexec_execution::storage::table_storage::Projections;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::{FileProvider, FileSource, FileSourceExt};
use rayexec_parquet::metadata::Metadata;
use rayexec_parquet::reader::AsyncBatchReader;

use crate::spec::{
    DataFile,
    Manifest,
    ManifestContent,
    ManifestEntryStatus,
    ManifestList,
    Snapshot,
    TableMetadata,
};

#[derive(Debug)]
pub struct Table {
    /// Root of the table.
    root: FileLocation,
    /// Provider for accessing files that make up the table.
    provider: Arc<dyn FileProvider>,
    /// Access configuration used with the provider.
    conf: AccessConfig,
    /// Deserialized table metadata.
    metadata: TableMetadata,
    /// Resolve paths relative to the table's root.
    resolver: PathResolver,
    /// Loaded manifest files.
    manifests: Vec<Manifest>,
}

impl Table {
    pub async fn load(
        root: FileLocation,
        provider: Arc<dyn FileProvider>,
        conf: AccessConfig,
    ) -> Result<Self> {
        let hint_path = root.join(["metadata", "version-hint.text"])?;

        let version_path = match provider.file_source(hint_path, &conf) {
            Ok(mut src) => {
                let hint_buf = src.read_stream_all().await?;
                let hint_str = str::from_utf8(&hint_buf)
                    .context("Expected version hint contents to be valid utf8")?;

                let line = if let Some((first, _)) = hint_str.split_once('\n') {
                    first
                } else {
                    hint_str
                };

                root.join(["metadata", &format!("v{}.metadata.json", line)])?
            }
            Err(_) => {
                // TODO: This could error for a variety of reasons, current just
                // assuming that it means the version hint file doesn't exist.
                //
                // List all the metadata files and try to get the one with the
                // latest version.

                let prefix = root.join(["metadata"])?;
                let mut metadata_stream = provider.list_prefix(prefix, &conf);

                let (mut latest, mut latest_rel_path) = (0_u32, None);

                while let Some(result) = metadata_stream.next().await {
                    let rel_paths = result?;

                    for rel_path in rel_paths {
                        // Extract version 43 from "v43.metadata.json"
                        if let Some(vers) = rel_path
                            .strip_prefix("v")
                            .and_then(|s| s.strip_suffix(".metadata.json"))
                        {
                            if let Ok(vers) = vers.parse::<u32>() {
                                if vers > latest {
                                    latest = vers;
                                    latest_rel_path = Some(rel_path);
                                }
                            }
                        }
                    }
                }

                let rel_path = latest_rel_path.ok_or_else(|| {
                    RayexecError::new(format!("No valid iceberg tables in root '{root}'"))
                })?;

                root.join(rel_path.split('/'))?
            }
        };

        let metadata_buf = provider
            .file_source(version_path, &conf)?
            .read_stream_all()
            .await?;
        let metadata: TableMetadata = serde_json::from_slice(&metadata_buf)
            .context("failed to deserialize table metadata")?;

        let resolver = PathResolver::from_metadata(&metadata);

        let mut table = Table {
            root,
            provider,
            conf,
            metadata,
            resolver,
            manifests: Vec::new(),
        };

        // TODO: Could be lazy, also we'll probably want to cache the table
        // metadata to reuse across queries.
        let manifests = table.read_manifests().await?;
        table.manifests = manifests;

        Ok(table)
    }

    pub fn scan(&self, projections: Projections, num_partitions: usize) -> Result<Vec<TableScan>> {
        // Find all data files in the manifests. We'll distribute these evenly
        // over however many partitions we need.
        let data_files_iter = self
            .manifests
            .iter()
            .filter(|m| matches!(m.metadata.content, ManifestContent::Data))
            .flat_map(|m| {
                m.entries.iter().filter_map(|ent| {
                    let status: ManifestEntryStatus = ent.status.try_into().unwrap_or_default();
                    if status.is_deleted() {
                        // Ignore deleted entries during table scans.
                        None
                    } else {
                        Some(&ent.data_file)
                    }
                })
            });

        let mut partitioned_files: Vec<_> = (0..num_partitions).map(|_| VecDeque::new()).collect();

        for (idx, data_file) in data_files_iter.enumerate() {
            // TODO: More formats?
            if !data_file.file_format.eq_ignore_ascii_case("parquet") {
                return Err(RayexecError::new( format!(
                    "'parquet' format currently the only supported file format for Iceberg, got '{}'", data_file.file_format,
                )));
            }

            let partition = idx % num_partitions;
            partitioned_files[partition].push_back(data_file.clone());
        }

        let schema = self.schema()?;

        let scans = partitioned_files
            .into_iter()
            .map(|files| TableScan {
                root: self.root.clone(),
                resolver: self.resolver.clone(),
                schema: schema.clone(),
                projections: projections.clone(),
                files,
                provider: self.provider.clone(),
                conf: self.conf.clone(),
                current: None,
            })
            .collect();

        Ok(scans)
    }

    pub fn schema(&self) -> Result<Schema> {
        let schema = self
            .metadata
            .schemas
            .iter()
            .find(|s| s.schema_id == self.metadata.current_schema_id)
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "Missing schema for id: {}",
                    self.metadata.current_schema_id
                ))
            })?;

        schema.to_schema()
    }

    async fn read_manifests(&self) -> Result<Vec<Manifest>> {
        let list = self.read_manifest_list().await?;

        let mut manifests = Vec::new();
        for ent in list.entries {
            let manifest_path = self.resolver.relative_path(&ent.manifest_path);

            let path = self.root.join(manifest_path.split('/'))?;
            let bs = self
                .provider
                .file_source(path, &self.conf)?
                .read_stream_all()
                .await?;

            let cursor = Cursor::new(bs);

            let manifest = Manifest::from_raw_avro(cursor)?;
            manifests.push(manifest);
        }

        Ok(manifests)
    }

    async fn read_manifest_list(&self) -> Result<ManifestList> {
        let current_snapshot = self.current_snapshot()?;
        let manifest_list_path = self.resolver.relative_path(&current_snapshot.manifest_list);

        let path = self.root.join(manifest_list_path.split('/'))?;
        let bs = self
            .provider
            .file_source(path, &self.conf)?
            .read_stream_all()
            .await?;

        let cursor = Cursor::new(bs);
        let list = ManifestList::from_raw_avro(cursor)?;

        Ok(list)
    }

    /// Get the current snapshot form the table metadta.
    fn current_snapshot(&self) -> Result<&Snapshot> {
        let current_snapshot_id = self
            .metadata
            .current_snapshot_id
            .ok_or_else(|| RayexecError::new("Missing current snapshot id".to_string()))?;

        let current_snapshot = self
            .metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == current_snapshot_id)
            .ok_or_else(|| {
                RayexecError::new(format!("Missing snapshot for id: {}", current_snapshot_id))
            })?;

        Ok(current_snapshot)
    }
}

#[derive(Debug)]
pub struct TableScan {
    /// Root of the table.
    root: FileLocation,
    /// Relative path resolver.
    resolver: PathResolver,
    /// Output schema of the table.
    schema: Schema,
    /// Column projections.
    projections: Projections,
    /// Files this scan is responsible for.
    files: VecDeque<DataFile>,
    /// File provider for getting the actual file sources.
    provider: Arc<dyn FileProvider>,
    conf: AccessConfig,
    /// Current reader, initially empty and populated on first stream.
    ///
    /// Once a reader runs out, the next file is loaded, and gets placed here.
    current: Option<AsyncBatchReader<Box<dyn FileSource>>>,
}

impl TableScan {
    pub async fn read_next(&mut self) -> Result<Option<Batch2>> {
        loop {
            if self.current.is_none() {
                let file = match self.files.pop_front() {
                    Some(file) => file,
                    None => return Ok(None), // We're done
                };

                // Get the path of the file relative to the path in the tabl's
                // metadata. This let's us do the path join below without any
                // issue as it'll already have the root in it.
                let path = self.resolver.relative_path(&file.file_path);

                let location = self.root.join(path.split('/'))?;

                self.current = Some(
                    Self::load_reader(
                        location,
                        &self.conf,
                        self.provider.as_ref(),
                        &self.schema,
                        self.projections.clone(),
                    )
                    .await?,
                )
            }

            match self.current.as_mut().unwrap().read_next().await {
                Ok(Some(batch)) => return Ok(Some(batch)),
                Ok(None) => {
                    // Loads next read at beginning of loop.
                    self.current = None;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }

    async fn load_reader(
        location: FileLocation,
        conf: &AccessConfig,
        provider: &dyn FileProvider,
        schema: &Schema,
        projections: Projections,
    ) -> Result<AsyncBatchReader<Box<dyn FileSource>>> {
        let mut source = provider.file_source(location, conf)?;

        let size = source.size().await?;
        let metadata = Arc::new(Metadata::new_from_source(source.as_mut(), size).await?);
        let row_groups: VecDeque<_> = (0..metadata.decoded_metadata.row_groups().len()).collect();

        const BATCH_SIZE: usize = 4096; // TODO
        let reader = AsyncBatchReader::try_new(
            source,
            row_groups,
            metadata,
            schema,
            BATCH_SIZE,
            projections,
        )?;

        Ok(reader)
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
