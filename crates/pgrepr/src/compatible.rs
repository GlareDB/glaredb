/// Variables desribing the postgres version glaredb claims to be compatible with.
use const_format::concatcp;

pub const PG_MAJOR_VERSION: u16 = 15;
pub const PG_MINOR_VERSION: u16 = 1;

/// A short-form version string for use in the `server_version` session
/// variable.
pub const fn server_version() -> &'static str {
    concatcp!(PG_MAJOR_VERSION, ".", PG_MINOR_VERSION)
}

/// A long-form version string for use in the `pg_catalog.version()` function.
///
/// Example:
/// PostgreSQL 15.1 on aarch64-unknown-linux-gnu, compiled by rustc (GlareDB 0.8.1)
pub const fn server_version_with_build_info() -> &'static str {
    // TODO: We should have a build_info crate with these constants.
    const GLAREDB_VERSION: &str = env!("CARGO_PKG_VERSION");
    const TARGET_TRIPLE: &str = "aarch64-unknown-linux-gnu"; // Again, build_info crate.

    concatcp!(
        "PostgreSQL ",
        server_version(),
        " on ",
        TARGET_TRIPLE,
        ", compiled by rustc (GlareDB ",
        GLAREDB_VERSION,
        ")"
    )
}
