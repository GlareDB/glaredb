use std::sync::Arc;

use glaredb_core::expr;
use glaredb_core::functions::table::TableFunctionInput;
use glaredb_core::functions::table::scan::ScanContext;
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::runtime::filesystem::file_provider::{MultiFileData, MultiFileProvider};
use glaredb_core::runtime::filesystem::{FileSystemWithState, OpenFlags};
use glaredb_error::{DbError, Result, ResultExt};

use crate::table::spec;

#[derive(Debug)]
pub struct TableState {
    pub fs: FileSystemWithState,
    pub metadata: Arc<spec::Metadata>,
    pub manifest_list: Option<Arc<spec::ManifestList>>,
    pub manifests: Option<Arc<Vec<spec::Manifest>>>,
}

impl TableState {
    // TODO: Not sure if requiring a scan context and args directly is what
    // we'll want long-term since a catalog will be providing some of this
    // input.
    //
    // But honestly this isn't too bad for the function entry point since it
    // handles auth params too.
    pub async fn open_root_with_inputs(
        scan_context: ScanContext<'_>,
        mut input: TableFunctionInput,
    ) -> Result<Self> {
        // Build the metadata glob by getting the table root (first arg) and
        // optionally a "version" argument.
        let root = ConstFold::rewrite(input.positional[0].clone())?
            .try_into_scalar()?
            .try_into_string()?;
        let version = match input.named.get("version") {
            Some(version) => Some(
                ConstFold::rewrite(version.clone())?
                    .try_into_scalar()?
                    .try_into_string()?,
            ),
            None => None,
        };

        let glob = format_glob_for_metadata(&root, version.as_deref());
        // Replace first arg with the glob.
        input.positional[0] = expr::lit(glob).into();

        // Now mf provider and filesystem.
        let (mut mf_prov, fs) =
            MultiFileProvider::try_new_from_inputs(scan_context, &input).await?;
        let mut mf_data = MultiFileData::empty();
        // Read all metadata files according to the glob.
        mf_prov.expand_all(&mut mf_data).await?;

        // Get the "max" metadata path.
        let metadata_path = match mf_data.expanded().iter().max() {
            Some(max) => max,
            None => {
                return Err(DbError::new(
                    "Could not find any metadata files in the table root",
                ));
            }
        };

        // Now read it.
        let mut file = fs.open(OpenFlags::READ, metadata_path).await?;
        let mut read_buf = vec![0; file.call_size() as usize];
        file.call_read_exact(&mut read_buf).await?;

        let metadata: spec::Metadata = serde_json::from_slice(&read_buf)
            .context_fn(|| format!("Failed to read metadata from {metadata_path}"))?;

        Ok(TableState {
            fs,
            metadata: Arc::new(metadata),
            manifest_list: None,
            manifests: None,
        })
    }

    pub async fn load_manifest_list(&mut self) -> Result<&spec::ManifestList> {
        let curr_snap = self.current_snapshot()?;
        let manifest_list_path = curr_snap
            .manifest_list
            .as_ref()
            .ok_or_else(|| DbError::new("Missing manifest list location for current snapshot"))?;
        // TODO: Make this an option if someone _really_ wants the absolute
        // path.
        let manifest_list_rel = relative_path(&self.metadata.location, manifest_list_path);
        let path = format!("{}/{}", self.metadata.location, manifest_list_rel);

        let mut file = self.fs.open(OpenFlags::READ, &path).await?;
        let mut read_buf = vec![0; file.call_size() as usize];
        file.call_read_exact(&mut read_buf).await?;

        let list = spec::ManifestList::from_raw_avro(&read_buf)?;
        self.manifest_list = Some(Arc::new(list));

        Ok(self.manifest_list.as_ref().unwrap())
    }

    pub async fn load_manifests(&mut self) -> Result<&[spec::Manifest]> {
        let list = match self.manifest_list.as_ref() {
            Some(list) => list,
            None => {
                return Err(DbError::new(
                    "Manifest list must be loaded before reading manifests",
                ));
            }
        };

        let mut manifests = Vec::with_capacity(list.entries.len());
        let mut read_buf = Vec::new();

        // TODO: Possibly concurrent reads.

        for ent in &list.entries {
            let manifest_rel = relative_path(&self.metadata.location, &ent.manifest_path);
            let path = format!("{}/{}", self.metadata.location, manifest_rel);

            let mut file = self.fs.open(OpenFlags::READ, &path).await?;
            read_buf.resize(file.call_size() as usize, 0);
            file.call_read_exact(&mut read_buf).await?;

            let manifest = spec::Manifest::from_raw_avro(&read_buf)?;
            manifests.push(manifest);
        }

        self.manifests = Some(Arc::new(manifests));

        Ok(self.manifests.as_ref().unwrap())
    }

    fn current_snapshot(&self) -> Result<&spec::Snapshot> {
        // TODO: Handle v1
        let curr_id = self
            .metadata
            .current_snapshot_id
            .ok_or_else(|| DbError::new("Missing current snapshot id"))?;

        let curr_snap = self
            .metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == curr_id)
            .ok_or_else(|| DbError::new(format!("Missing snapshot for id {curr_id}")))?;

        Ok(curr_snap)
    }
}

/// Formats a glob to use for listing the metadata json files for a table.
fn format_glob_for_metadata(root: &str, version: Option<&str>) -> String {
    let root = root.trim_end_matches("/");
    // Metadata formats:
    // - table_root/metadata/<version>-<uuid>.metadata.json
    // - table_root/metadata/<version>-<uuid>.gz.metadata.json
    //
    // Note that there's nothing in the spec about if <version> should be just
    // an int, or be prefixed with 'v'. pyiceberg seems to generate metadata
    // files without the 'v', so let's just say that's part of the "version".
    //
    // Also we somehow have tables that don't have uuids in the metadata name,
    // so the entire metadata file name is something like 'v2.metadata.json'. I
    // don't know if that's valid or not.
    match version {
        Some(version) => format!("{root}/metadata/{version}*.metadata.json"),
        None => {
            // No version, we'll be globbing everything in metadata, and find
            // the lexicographically greatest file as the latest.
            format!("{root}/metadata/*.metadata.json")
        }
    }
}

/// Get the relative path for a file according to the table's root.
///
/// File paths in the table metadata and manifests will include the base table's
/// location, so we need to remove that when accessing those files.
///
/// E.g.
/// manifest-list => out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro
/// root          => out/iceberg_table
///
/// This should give us:
/// metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro
fn relative_path<'a>(root: &str, path: &'a str) -> &'a str {
    // TODO: We'll probably want some better path resolution here. I'm not
    // sure what all is allowed for metadata location.

    // Are people really ok with absolute paths in the metadata? wtf?

    // Remove leading "./" from metadata location
    let metadata_location = root.trim_start_matches("./");

    // Remove metadata location from path that was passed in.
    path.trim_start_matches(metadata_location).trim_matches('/')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_glob_cases() {
        struct TestCase {
            root: &'static str,
            version: Option<&'static str>,
            expected: &'static str,
        }

        let cases = [
            TestCase {
                root: "wh/default.db",
                version: None,
                expected: "wh/default.db/metadata/*.metadata.json",
            },
            TestCase {
                root: "wh/default.db",
                version: Some("00001"),
                expected: "wh/default.db/metadata/00001*.metadata.json",
            },
            TestCase {
                root: "wh/default.db/", // Trailing slash
                version: Some("00001"),
                expected: "wh/default.db/metadata/00001*.metadata.json",
            },
        ];

        for case in cases {
            let got = format_glob_for_metadata(case.root, case.version);
            assert_eq!(case.expected, got);
        }
    }

    #[test]
    fn relative_path_cases() {
        struct TestCase {
            root: &'static str,
            input: &'static str,
            expected: &'static str,
        }

        let test_cases = vec![
            // Relative table location
            TestCase {
                root: "out/iceberg_table",
                input: "out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
                expected: "metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
            },
            // Relative table location with "./"
            TestCase {
                root: "./out/iceberg_table",
                input: "out/iceberg_table/metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
                expected: "metadata/snap-4160073268445560424-1-095d0ad9-385f-406f-b29c-966a6e222e58.avro",
            },
            // Absolute table location
            TestCase {
                root: "/Users/sean/Code/github.com/glaredb/glaredb/testdata/iceberg/tables/lineitem_versioned",
                input: "/Users/sean/Code/github.com/glaredb/glaredb/testdata/iceberg/tables/lineitem_versioned/metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
                expected: "metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
            },
            // s3 table location
            TestCase {
                root: "s3://testdata/iceberg/tables/lineitem_versioned",
                input: "s3://testdata/iceberg/tables/lineitem_versioned/metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
                expected: "metadata/snap-2591356646088336681-1-481f5867-e369-4c1c-a9ba-6c9e04030958.avro",
            },
        ];

        for case in test_cases {
            let out = relative_path(case.root, case.input);
            assert_eq!(case.expected, out, "root: {}", case.root,);
        }
    }
}
