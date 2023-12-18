use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::fmt::Display;

/// Use python's builtin `print` to display an item.
pub fn pyprint(item: impl Display, py: Python) -> PyResult<()> {
    let locals = PyDict::new(py);
    locals.set_item("__display_item", item.to_string())?;
    py.run("print(__display_item)", None, Some(locals))?;
    Ok(())
}
