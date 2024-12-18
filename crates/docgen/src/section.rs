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
