use pyo3::{pyclass, pymethods, Python};
use rayexec_shell::result_table::MaterializedResultTable;

use crate::errors::Result;
use crate::print::pyprint;

const DEFAULT_TABLE_WIDTH: usize = 100;

// TODO: Lazy
#[pyclass]
#[derive(Debug)]
pub struct PythonMaterializedResultTable {
    pub(crate) table: MaterializedResultTable,
}

#[pymethods]
impl PythonMaterializedResultTable {
    fn __repr__(&self) -> Result<String> {
        let pretty = self.table.pretty_table(DEFAULT_TABLE_WIDTH, None)?;
        Ok(format!("{pretty}"))
    }

    fn show(&self, py: Python) -> Result<()> {
        let pretty = self.table.pretty_table(DEFAULT_TABLE_WIDTH, None)?;
        pyprint(pretty, py)
    }

    /// Prints out the profiling data for the query.
    ///
    /// Currently just used for debugging/perf testings.
    fn dump_profile(&self, py: Python) -> Result<()> {
        pyprint("---- PLANNING ----", py)?;
        match self.table.planning_profile_data() {
            Some(data) => pyprint(data, py)?,
            None => pyprint("N/A", py)?,
        }

        pyprint("---- EXECUTION ----", py)?;
        match self.table.execution_profile_data() {
            Some(data) => pyprint(data, py)?,
            None => pyprint("N/A", py)?,
        }

        Ok(())
    }
}
