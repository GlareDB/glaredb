use rayexec_error::{RayexecError, Result};

use crate::array::Array;

/// Behavior when a cast fail due to under/overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastFailBehavior {
    /// Return an error.
    Error,
    /// Use a NULL value.
    Null,
}

impl CastFailBehavior {
    pub(crate) fn new_state_for_array(&self, _arr: &Array) -> CastFailState {
        match self {
            CastFailBehavior::Error => CastFailState::TrackOneAndError(None),
            CastFailBehavior::Null => CastFailState::TrackManyAndInvalidate(Vec::new()),
        }
    }
}

#[derive(Debug)]
pub struct ErrorIndex {
    /// Row index that we failed on.
    pub idx: usize,
    /// Optional error we can use instead of the generic "failed to cast" error.
    pub error: Option<RayexecError>,
}

/// State used to track failures casting.
#[derive(Debug)]
pub(crate) enum CastFailState {
    /// Keep the row index of the first failure.
    TrackOneAndError(Option<ErrorIndex>),
    /// Track all failures during casting.
    TrackManyAndInvalidate(Vec<usize>),
}

impl CastFailState {
    pub(crate) fn set_did_fail(&mut self, idx: usize) {
        match self {
            Self::TrackOneAndError(maybe_idx) => {
                if maybe_idx.is_none() {
                    *maybe_idx = Some(ErrorIndex { idx, error: None });
                }
            }
            Self::TrackManyAndInvalidate(indices) => indices.push(idx),
        }
    }

    pub(crate) fn set_did_fail_with_error(&mut self, idx: usize, error: RayexecError) {
        match self {
            Self::TrackOneAndError(maybe_idx) => {
                if maybe_idx.is_none() {
                    *maybe_idx = Some(ErrorIndex {
                        idx,
                        error: Some(error),
                    })
                }
            }
            Self::TrackManyAndInvalidate(indices) => indices.push(idx), // Error ignored, we're replacing with null.
        }
    }

    pub(crate) fn check_and_apply(self, original: &Array, mut output: Array) -> Result<Array> {
        match self {
            Self::TrackOneAndError(None) => Ok(output),
            Self::TrackOneAndError(Some(error_idx)) => {
                let scalar = original.logical_value(error_idx.idx)?;
                match error_idx.error {
                    Some(error) => Err(RayexecError::with_source(
                        format!("Failed to cast '{scalar}' to {}", output.datatype()),
                        Box::new(error),
                    )),
                    None => Err(RayexecError::new(format!(
                        "Failed to cast '{scalar}' to {}",
                        output.datatype()
                    ))),
                }
            }
            Self::TrackManyAndInvalidate(indices) => {
                if indices.is_empty() {
                    Ok(output)
                } else {
                    // Apply the nulls.
                    for idx in indices {
                        output.set_physical_validity(idx, false);
                    }
                    Ok(output)
                }
            }
        }
    }
}
