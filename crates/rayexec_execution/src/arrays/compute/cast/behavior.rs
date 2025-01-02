use rayexec_error::{RayexecError, Result};

/// Behavior when a cast fail due to under/overflow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastFailBehavior {
    /// Return an error.
    Error,
    /// Use a NULL value.
    Null,
}

impl CastFailBehavior {
    pub(crate) fn new_state(&self) -> CastErrorState {
        CastErrorState {
            behavior: *self,
            error: None,
        }
    }
}

#[derive(Debug)]
pub struct CastErrorState {
    behavior: CastFailBehavior,
    error: Option<RayexecError>,
}

impl CastErrorState {
    /// Set the error from a function.
    ///
    /// If the cast behavior is use NULL on failure, then `error_fn` is not
    /// called.
    pub fn set_error<F>(&mut self, error_fn: F)
    where
        F: FnOnce() -> RayexecError,
    {
        if self.behavior == CastFailBehavior::Error && self.error.is_none() {
            self.error = Some(error_fn())
        }
    }

    pub fn into_result(self) -> Result<()> {
        match self.error {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
