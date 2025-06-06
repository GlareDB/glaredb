use std::fmt;

use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::arrays::format::{BinaryFormat, FormatOptions, Formatter};
use glaredb_error::Result;

const FORMATTER: Formatter = Formatter::new(FormatOptions {
    null: "",
    empty_string: "",
    binary_format: BinaryFormat::Hex,
});

#[allow(unused)]
pub fn write_markdown_table<'a>(
    output: &mut dyn fmt::Write,
    schema: &ColumnSchema,
    batches: impl IntoIterator<Item = &'a Batch>,
) -> Result<()> {
    // 'field1 | field2 | field3'
    let header = schema
        .fields
        .iter()
        .map(|f| f.name.clone())
        .collect::<Vec<_>>()
        .join(" | ");

    writeln!(output, "| {header} |")?;

    // ' --- | --- | ---'
    let sep = schema
        .fields
        .iter()
        .map(|_| "---")
        .collect::<Vec<_>>()
        .join(" | ");

    writeln!(output, "| {sep} |")?;

    for batch in batches {
        for row in 0..batch.num_rows() {
            for (idx, column) in batch.arrays().iter().enumerate() {
                if idx == 0 {
                    write!(output, "|")?;
                }

                let val = FORMATTER.format_array_value(column, row)?;
                write!(output, " {val} |")?;
            }
            writeln!(output)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use glaredb_core::arrays::datatype::DataType;
    use glaredb_core::arrays::field::Field;
    use glaredb_core::generate_batch;

    use super::*;

    #[test]
    fn simple() {
        let batch = generate_batch!([1, 2, 3], ["cat", "dog", "mouse"]);

        let schema = ColumnSchema::new([
            Field::new("Numbers", DataType::int32(), false),
            Field::new("Strings", DataType::utf8(), false),
        ]);

        let mut buf = String::new();

        write_markdown_table(&mut buf, &schema, [&batch]).unwrap();

        let expected = r#"| Numbers | Strings |
| --- | --- |
| 1 | cat |
| 2 | dog |
| 3 | mouse |
"#;

        assert_eq!(expected, buf);
    }
}
