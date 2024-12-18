use std::fmt::{self, Debug};

use rayexec_error::Result;

use crate::markdown_table::write_markdown_table;
use crate::session::DocsSession;

pub trait SectionWriter: Debug {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()>;
}

const SCALAR_FUNCTIONS_QUERY: &str = r#"
SELECT
    function_name as "Function name",
    description as "Description"
FROM list_functions()
WHERE function_type = 'scalar'
GROUP BY "Function name", "Description"
ORDER BY "Function name";
"#;

#[derive(Debug)]
pub struct ScalarFunctionWriter;

impl SectionWriter for ScalarFunctionWriter {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()> {
        let table = session.query(SCALAR_FUNCTIONS_QUERY)?;
        write_markdown_table(output, table.schema(), table.iter_batches())
    }
}

const AGGREGATE_FUNCTIONS_QUERY: &str = r#"
SELECT
    function_name as "Function name",
    description as "Description"
FROM list_functions()
WHERE function_type = 'aggregate'
GROUP BY "Function name", "Description"
ORDER BY "Function name";
"#;

#[derive(Debug)]
pub struct AggregateFunctionWriter;

impl SectionWriter for AggregateFunctionWriter {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()> {
        let table = session.query(AGGREGATE_FUNCTIONS_QUERY)?;
        write_markdown_table(output, table.schema(), table.iter_batches())
    }
}

const TABLE_FUNCTIONS_QUERY: &str = r#"
SELECT
    function_name as "Function name",
    description as "Description"
FROM list_functions()
WHERE function_type = 'table'
GROUP BY "Function name", "Description"
ORDER BY "Function name";
"#;

#[derive(Debug)]
pub struct TableFunctionWriter;

impl SectionWriter for TableFunctionWriter {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()> {
        let table = session.query(TABLE_FUNCTIONS_QUERY)?;
        write_markdown_table(output, table.schema(), table.iter_batches())
    }
}
