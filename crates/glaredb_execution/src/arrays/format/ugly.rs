use std::fmt::Write as _;

use glaredb_error::Result;

use crate::arrays::batch::Batch;
use crate::arrays::field::ColumnSchema;
use crate::arrays::format::{FormatOptions, Formatter};

pub fn ugly_format_no_schema<'a, I>(batches: I) -> Result<String>
where
    I: IntoIterator<Item = &'a Batch>,
{
    const OPTS: FormatOptions = FormatOptions::new();
    let formatter = Formatter::new(OPTS);

    let mut buf = String::new();

    for batch in batches.into_iter() {
        for idx in 0..batch.num_rows() {
            for (col_idx, col) in batch.arrays().iter().enumerate() {
                write!(
                    buf,
                    "{}\t ",
                    formatter
                        .format_array_value(col, idx)
                        .expect("value to exist")
                )?;
                if col_idx < batch.arrays().len() - 1 {
                    write!(buf, "| ")?;
                }
            }
            if idx < batch.num_rows() - 1 {
                writeln!(buf)?;
            }
        }
    }

    Ok(buf)
}

pub fn ugly_format<'a, I>(schema: &ColumnSchema, batches: I) -> Result<String>
where
    I: IntoIterator<Item = &'a Batch>,
{
    const OPTS: FormatOptions = FormatOptions::new();
    let formatter = Formatter::new(OPTS);

    let mut buf = schema
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>()
        .join("\t");

    for batch in batches.into_iter() {
        for idx in 0..batch.num_rows() {
            for (col_idx, col) in batch.arrays().iter().enumerate() {
                write!(
                    buf,
                    "{}\t ",
                    formatter
                        .format_array_value(col, idx)
                        .expect("value to exist")
                )?;
                if col_idx < batch.arrays().len() - 1 {
                    write!(buf, "| ")?;
                }
            }
            if idx < batch.num_rows() - 1 {
                writeln!(buf)?;
            }
        }
    }

    Ok(buf)
}
