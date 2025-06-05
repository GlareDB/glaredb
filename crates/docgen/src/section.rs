use std::fmt::{self, Debug};

use glaredb_core::arrays::format::{BinaryFormat, FormatOptions, Formatter};
use glaredb_core::functions::documentation::Category;
use glaredb_error::Result;

use crate::session::DocsSession;

pub trait SectionWriter: Debug {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()>;
}

#[derive(Debug)]
pub struct FunctionSectionWriter<const HEADER_LEVEL: usize> {
    pub category: Category,
}

impl<const HEADER_LEVEL: usize> SectionWriter for FunctionSectionWriter<HEADER_LEVEL> {
    fn write(&self, session: &DocsSession, output: &mut dyn fmt::Write) -> Result<()> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions {
            null: "",
            empty_string: "",
            binary_format: BinaryFormat::Hex,
        });

        let query = format!(
            r#"
            SELECT DISTINCT
              function_name,
              alias_of,
              description,
              example,
              example_output
            FROM
              list_functions()
            WHERE category = '{}'
            ORDER BY 1,2,3,4,5;
            "#,
            self.category.as_str(),
        );

        let result = session.query(&query)?;

        let header_prefix = "#".repeat(HEADER_LEVEL);

        for batch in result.batches {
            for row in 0..batch.num_rows() {
                let function_name = FORMATTER.format_array_value(&batch.arrays()[0], row)?;
                let alias_of = batch.arrays()[1].get_value(row)?;
                let description = FORMATTER.format_array_value(&batch.arrays()[2], row)?;

                writeln!(output, "{header_prefix} `{}`\n", function_name)?;
                if !alias_of.is_null() {
                    writeln!(output, "**Alias of `{alias_of}`**\n")?;
                }

                writeln!(output, "{}\n", description)?;

                let example = batch.arrays()[3].get_value(row)?;
                let example_output = batch.arrays()[4].get_value(row)?;
                if !example.is_null() {
                    writeln!(output, "**Example**: `{}`\n", example)?;
                    writeln!(output, "**Output**: `{}`\n", example_output)?;
                }
            }
        }

        Ok(())
    }
}
