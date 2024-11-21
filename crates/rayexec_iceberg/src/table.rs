use core::str;
use std::sync::Arc;

use futures::StreamExt;
use rayexec_bullet::field::Schema;
use rayexec_error::{RayexecError, Result, ResultExt};
use rayexec_execution::storage::table_storage::Projections;
use rayexec_io::location::{AccessConfig, FileLocation};
use rayexec_io::{FileProvider, FileSourceExt};

use crate::spec::{Manifest, ManifestList, Snapshot, TableMetadata};

const VERSION_HINT_PATH: &'static str = "metadata/version-hint.text";

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
}

impl Table {
    pub async fn load(
        root: FileLocation,
        provider: Arc<dyn FileProvider>,
        conf: AccessConfig,
    ) -> Result<Self> {
        let hint_path = root.join([VERSION_HINT_PATH])?;

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

                root.join([format!("metadata/v{}.metadata.json", line)])?
            }
            Err(_) => {
                // TODO: This could error for a variety of reasons, current just
                // assuming that it means the version hint file doesn't exist.
                //
                // List all the metadata files and try to get the one with the
                // latest version.

                let prefix = root.join(["metadata/"])?;
                let mut metadata_stream = provider.list_prefix(prefix, &conf);

                let (mut latest, mut latest_rel_path) = (0_u32, None);

                while let Some(result) = metadata_stream.next().await {
                    let rel_paths = result?;

                    for rel_path in rel_paths {
                        // Extract version 43 from "v43.metadata.json"
                        if let Some(vers) = rel_path
                            .strip_prefix("v")
                            .map(|s| s.strip_suffix(".metadata.json"))
                            .flatten()
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
                    RayexecError::new(format!("No valid iceberg tables in root {root}"))
                })?;

                root.join([rel_path])?
            }
        };

        let metadata_buf = provider
            .file_source(version_path, &conf)?
            .read_stream_all()
            .await?;
        let metadata: TableMetadata = serde_json::from_slice(&metadata_buf)
            .context("failed to deserialize table metadata")?;

        let resolver = PathResolver::from_metadata(&metadata);

        Ok(Table {
            root,
            provider,
            conf,
            metadata,
            resolver,
        })
    }

    pub fn scan(&self, projections: Projections, num_partitions: usize) -> Result<Vec<TableScan>> {
        unimplemented!()
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
        unimplemented!()
    }

    async fn read_manifest_list(&self) -> Result<ManifestList> {
        unimplemented!()
    }

    /// Get the current snapshot form the table metadta.
    fn current_snapshot(&self) -> Result<&Snapshot> {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct TableScan {}

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
