use std::ffi::CStr;
use std::fmt::Display;

use pyo3::ffi::c_str;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::errors::Result;

/// Use python's builtin `print` to display an item.
// TODO: Grab stdio/err directly instead of needing to convert to a string and
// call print on the python side.
pub fn pyprint(item: impl Display, py: Python) -> Result<()> {
    let locals = PyDict::new(py);
    locals.set_item("__display_item", item.to_string())?;

    const PRINT: &CStr = c_str!("print(__display_item)");
    py.run(PRINT, None, Some(&locals))?;

    Ok(())
}
