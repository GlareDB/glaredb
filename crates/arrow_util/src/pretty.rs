use comfy_table::{Cell, ColumnConstraint, ContentArrangement, Table};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use once_cell::sync::Lazy;
use std::fmt::Display;

const DEFAULT_PRESET: &str = "││──╞═╪╡│    ┬┴┌┐└┘";

static TABLE_FORMAT_OPTS: Lazy<FormatOptions> = Lazy::new(|| {
    FormatOptions::default()
        .with_display_error(false)
        .with_null("NULL")
});

/// Pretty format record batches.
pub fn pretty_format_batches(
    batches: &[RecordBatch],
    width: Option<usize>,
    max_rows: Option<usize>,
) -> Result<impl Display, ArrowError> {
    create_table(batches, &TABLE_FORMAT_OPTS, width, max_rows)
}

fn create_table(
    batches: &[RecordBatch],
    opts: &FormatOptions,
    width: Option<usize>,
    max_rows: Option<usize>,
) -> Result<Table, ArrowError> {
    let mut table = Table::new();
    table.load_preset(DEFAULT_PRESET);

    table.set_content_arrangement(ContentArrangement::Dynamic);

    if let Some(width) = width {
        table.set_width(width as u16);
    }

    if batches.is_empty() {
        return Ok(table);
    }

    let header: Vec<_> = batches[0]
        .schema()
        .fields()
        .iter()
        .map(|f| Cell::new(format!("{}\n{}", f.name(), f.data_type())))
        .collect();
    let num_cols = header.len();
    table.set_header(header);

    table.set_constraints(
        std::iter::repeat(ColumnConstraint::LowerBoundary(comfy_table::Width::Fixed(
            10,
        )))
        .take(num_cols),
    );

    for batch in batches {
        let formatters = batch
            .columns()
            .iter()
            .map(|c| ArrayFormatter::try_new(c.as_ref(), opts))
            .collect::<Result<Vec<_>, ArrowError>>()?;

        for row in 0..batch.num_rows() {
            let cells: Vec<_> = formatters.iter().map(|f| Cell::new(f.value(row))).collect();
            table.add_row(cells);
        }
    }

    Ok(table)
}
