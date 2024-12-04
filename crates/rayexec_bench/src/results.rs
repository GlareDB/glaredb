use rayexec_bullet::format::{FormatOptions, Formatter};
use rayexec_error::{RayexecError, Result};
use rayexec_shell::result_table::MaterializedResultTable;

pub fn compare_results(expected_rows: &[String], table: MaterializedResultTable) -> Result<()> {
    let got_rows = table_to_rows(table)?;

    if expected_rows.len() != got_rows.len() {
        return Err(format_mismatch_error(expected_rows, got_rows));
    }

    for (expected, got) in expected_rows.iter().zip(&got_rows) {
        let mut remaining = expected.as_str();
        for col in got {
            remaining = remaining.trim_start_matches(col);
            remaining = remaining.trim_start()
        }

        remaining = remaining.trim_end();
        if !remaining.is_empty() {
            return Err(format_mismatch_error(expected_rows, got_rows));
        }
    }

    Ok(())
}

fn format_mismatch_error(expected: &[String], got: Vec<Vec<String>>) -> RayexecError {
    RayexecError::new(format!(
        "Results mismatch. \nExpected:\n{}\n\nGot:\n{}\n",
        expected.join("\n"),
        got.into_iter()
            .map(|row| row.join(" "))
            .collect::<Vec<_>>()
            .join("\n"),
    ))
}

/// Convert a materialized table to row strings.
pub fn table_to_rows(table: MaterializedResultTable) -> Result<Vec<Vec<String>>> {
    const OPTS: FormatOptions = FormatOptions {
        null: "NULL",
        empty_string: "(empty)",
    };
    let formatter = Formatter::new(OPTS);

    let mut rows = Vec::new();

    for row in table.iter_rows() {
        let col_strings: Vec<_> = row
            .iter()
            .map(|col| formatter.format_scalar_value(col.clone()).to_string())
            .collect();

        rows.push(col_strings)
    }

    Ok(rows)
}
