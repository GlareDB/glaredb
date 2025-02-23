use std::collections::HashMap;

use rayexec_error::Result;

use crate::arrays::scalar::ScalarValue;

#[derive(Debug)]
pub struct FileList {
    /// Unexpanded file paths.
    unexpanded: Vec<String>,
    /// Expanded file paths.
    expanded: Vec<String>,
}

impl FileList {
    /// Create a new file list with all expanded paths.
    pub fn new_simple<S>(paths: impl IntoIterator<Item = S>) -> Self
    where
        S: Into<String>,
    {
        FileList {
            unexpanded: Vec::new(),
            expanded: paths.into_iter().map(|s| s.into()).collect(),
        }
    }

    pub fn expanded_file_count(&self) -> usize {
        self.expanded.len()
    }
}

#[derive(Debug)]
pub struct MultiFileScan {}

impl MultiFileScan {
    pub fn new_from_options(
        positional: &[ScalarValue],
        named: &HashMap<String, ScalarValue>,
    ) -> Result<Self> {
        unimplemented!()
    }
}
