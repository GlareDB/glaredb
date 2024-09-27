use pyo3::{pyclass, pymethods};
use rayexec_shell::result_table::MaterializedResultTable;

use crate::errors::Result;

// TODO: Lazy
#[pyclass]
#[derive(Debug)]
pub struct PythonMaterializedResultTable {
    pub(crate) table: MaterializedResultTable,
}

#[pymethods]
impl PythonMaterializedResultTable {
    fn __repr__(&mut self) -> Result<String> {
        let pretty = self.table.pretty_table(80, None)?;
        Ok(format!("{pretty}"))
    }
}
