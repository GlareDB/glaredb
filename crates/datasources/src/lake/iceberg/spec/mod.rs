//! Types related to the Iceberg table spec (not view spec).
//!
//! - Metadata files: JSON files describing the metada for a table. These are
//!   the "entry point" to reading the actual table data. Metadata files contain
//!   schemas, partitions specs, and snapshots. Snapshots provide a path to a
//!   manifest list.
//!
//! - Manifest list files: Avro files containing a list of manifests and
//!   additional metadata for a table.
//!
//! - Manifest files: Avro files that contain a list of data files.
//!
//! - Data files: The actual files containing the data for a table. These can be
//!   Parquet, ORC, or Avro. We only currently support Avro. Data files can
//!   either store data contents, or provide delete information for a table.
//!
//! Relevants text copied from the spec are prefixed with "> " (markdown quotes).

mod schema;
pub use schema::*;
