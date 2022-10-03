//! File readers and writers.
pub mod data;

/// An error occured when finishing some write to a file.
#[derive(Debug)]
pub struct FinishError<T> {
    pub writer: T,
    pub error: crate::errors::ExpstoreError,
}
