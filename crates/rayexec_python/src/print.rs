use crate::errors::Result;

use std::fmt::Display;

use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Use python's builtin `print` to display an item.
// TODO: Grab stdio/err directly instead of needing to convert to a string and
// call print on the python side.
pub fn pyprint(item: impl Display, py: Python) -> Result<()> {
    let locals = PyDict::new_bound(py);
    locals.set_item("__display_item", item.to_string())?;
    py.run_bound("print(__display_item)", None, Some(&locals))?;
    Ok(())
}
