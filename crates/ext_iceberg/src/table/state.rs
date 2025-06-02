use std::sync::Arc;

use glaredb_core::expr;
use glaredb_core::functions::table::TableFunctionInput;
use glaredb_core::functions::table::scan::ScanContext;
use glaredb_core::optimizer::expr_rewrite::ExpressionRewriteRule;
use glaredb_core::optimizer::expr_rewrite::const_fold::ConstFold;
use glaredb_core::runtime::filesystem::file_provider::{MultiFileData, MultiFileProvider};
use glaredb_core::runtime::filesystem::{FileSystemWithState, OpenFlags};
use glaredb_error::{DbError, Result, ResultExt, not_implemented};

use crate::table::spec;

#[derive(Debug)]
pub struct TableState {
    pub metadata: Arc<spec::Metadata>,
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
        let mut file = fs.open(OpenFlags::READ, &metadata_path).await?;
        let mut read_buf = vec![0; file.call_size() as usize];
        file.call_read_exact(&mut read_buf).await?;

        let metadata: spec::Metadata = serde_json::from_slice(&read_buf)
            .context_fn(|| format!("Failed to read metadata from {metadata_path}"))?;

        Ok(TableState {
            metadata: Arc::new(metadata),
        })
    }
}

fn format_glob_for_metadata(root: &str, version: Option<&str>) -> String {
    let root = root.trim_end_matches("/");
    // Metadata formats:
    // - table_root/metadata/<version>-<uuid>.metadata.json
    // - table_root/metadata/<version>-<uuid>.gz.metadata.json
    //
    // Note that there's nothing in the spec about if <version> should be just
    // an int, or be prefixed with 'v'. pyiceberg seems to generate metadata
    // files without the 'v', so let's just say that's part of the "version".
    match version {
        Some(version) => format!("{root}/metadata/{version}-*.metadata.json"),
        None => {
            // No version, we'll be globbing everything in metadata, and find
            // the lexicographically greatest file as the latest.
            format!("{root}/metadata/*.metadata.json")
        }
    }
}
