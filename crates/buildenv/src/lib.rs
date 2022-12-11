//! Crate for getting information about the build environment.

/// Get the git tag from the compilation environment.
pub const fn git_tag() -> &'static str {
    match option_env!("GIT_TAG") {
        Some(hash) => hash,
        None => "unknown",
    }
}
