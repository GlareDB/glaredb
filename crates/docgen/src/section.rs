use std::fmt::{self, Debug};

use glaredb_core::arrays::format::{FormatOptions, Formatter};
use glaredb_core::functions::documentation::Category;
use glaredb_error::Result;

use crate::session::DocsSession;

pub trait SectionWriter: Debug {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()>;
}

#[derive(Debug)]
pub struct FunctionSectionWriter {
    pub category: Category,
}

impl SectionWriter for FunctionSectionWriter {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions {
            null: "",
            empty_string: "",
        });

        let query = format!(
            r#"
            SELECT DISTINCT
              function_name,
              description
            FROM
              list_functions()
            WHERE category = '{}'
            ORDER BY function_name;
            "#,
            self.category.as_str(),
        );

        let result = session.query(&query)?;

        for batch in result.batches {
            for row in 0..batch.num_rows() {
                let function_name = FORMATTER.format_array_value(&batch.arrays()[0], row)?;
                let description = FORMATTER.format_array_value(&batch.arrays()[1], row)?;

                writeln!(output, "## {}\n", function_name)?;
                writeln!(output, "{}\n", description)?;
            }
        }

        Ok(())
    }
}
