pub mod args;
pub mod commands;
mod highlighter;
pub mod local;
pub mod metastore;
mod prompt;
pub mod proxy;
pub mod server;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}
