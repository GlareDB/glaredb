use std::collections::HashMap;
use std::fmt::Debug;

use glaredb_error::Result;

use crate::arrays::scalar::ScalarValue;

/// Describes configuration for accessing files.
pub trait AccessConfig: Debug + Sized {
    /// Construct an access config from the given unnamed and named options.
    fn from_options(unnamed: &[ScalarValue], named: &HashMap<String, ScalarValue>) -> Result<Self>;
}
