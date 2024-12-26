//! A suspiciously Arrow-like columnar storage implementation.
pub mod array;
pub mod batch;
pub mod bitmap;
pub mod compute;
pub mod datatype;
pub mod executor;
pub mod field;
pub mod format;
pub mod ipc;
pub mod row;
pub mod scalar;
pub mod selection;
pub mod storage;

pub mod testutil;

mod bitutil;
mod shared_or_owned;
