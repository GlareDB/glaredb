use comfy_table::{Cell, ColumnConstraint, ContentArrangement, Table};
use datafusion::arrow::datatypes::{DataType, Field, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use once_cell::sync::Lazy;
use std::fmt::Display;
use std::ops::Range;
use std::sync::Arc;

const DEFAULT_PRESET: &str = "││──╞═╪╡│    ┬┴┌┐└┘";
const DEFAULT_MAX_COLUMNS: usize = 8;
const DEFAULT_MAX_ROWS: usize = 20;
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
    max_columns: Option<usize>,
) -> Result<impl Display, ArrowError> {
    create_table(batches, &TABLE_FORMAT_OPTS, width, max_rows, max_columns)
}

fn default_table() -> Table {
    let mut table = Table::new();
    table.load_preset(DEFAULT_PRESET);
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table
}

fn create_table(
    batches: &[RecordBatch],
    opts: &FormatOptions,
    width: Option<usize>,
    max_rows: Option<usize>,
    max_columns: Option<usize>,
) -> Result<Table, ArrowError> {
    if batches.is_empty() || batches[0].schema().fields().is_empty() {
        return Ok(default_table());
    }
    let num_columns = batches[0].num_columns();
    let total_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>();
    let str_truncate = 32;
    let mut max_cols = max_columns.unwrap_or_else(|| {
        if let Some(width) = width {
            // sum the length of all the column names
            // and divide by the number of columns
            let avg_colum_len = batches[0]
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().len())
                .sum::<usize>()
                / num_columns;
            (width - 10) / avg_colum_len
        } else {
            DEFAULT_MAX_COLUMNS
        }
    });

    if max_cols == 0 || max_cols > num_columns {
        max_cols = num_columns;
    }
    let mut max_rows = max_rows.unwrap_or_else(|| std::cmp::min(total_rows, DEFAULT_MAX_ROWS));

    if max_rows == 0 || max_rows > total_rows {
        max_rows = total_rows;
    }
    // todo: make this configurable

    let mut table = default_table();

    if let Some(width) = width {
        table.set_width(width as u16);
    }

    table.set_constraints(
        std::iter::repeat(ColumnConstraint::LowerBoundary(comfy_table::Width::Fixed(
            10,
        )))
        .take(max_cols),
    );

    let mut processed_rows = 0;
    // if the number of columns is greater than the max columns, we split the columns into 2 halves
    // and we add a placeholder in the middle.
    // we try to evenly split from the beginning and end.
    let (n_first, n_last) = if num_columns > max_cols {
        ((max_cols + 1) / 2, max_cols / 2)
    } else {
        (num_columns, 0)
    };

    let reduce_columns = n_first + n_last < num_columns;
    let n_tbl_cols = n_first + n_last + reduce_columns as usize;

    let column_ranges = (0..n_first, (num_columns - n_last)..num_columns);

    let mut needs_split = false;
    let mut add_dots = false;
    let row_split = if max_rows >= total_rows {
        total_rows
    } else {
        let split = max_rows / 2;
        needs_split = true;
        add_dots = true;
        split
    };

    process_header(
        &mut table,
        &batches[0],
        column_ranges.clone(),
        reduce_columns,
        str_truncate,
    )?;
    let n_last_rows = total_rows - row_split;

    let mut tbl_rows = 0;

    for batch in batches {
        let num_rows = batch.num_rows();
        // if the batch is smaller than the row split upper bound, we can just process it in one go
        // otherwise, we need to process them in 2 halves. The first half is from 0..row_split, the second half is from (max_rows - (row_split +1))..max_rows
        // if we split it, we add a placeholder row in the middle
        if tbl_rows < row_split {
            let remaining_rows = row_split - tbl_rows;
            let rows_to_take = remaining_rows.min(num_rows);
            process_batch(
                &mut table,
                opts,
                batch,
                0..rows_to_take,
                column_ranges.clone(),
                reduce_columns,
            )?;
            tbl_rows += rows_to_take;
        }

        if tbl_rows >= row_split && add_dots {
            // Add continuation
            let dots: Vec<_> = (0..max_cols).map(|_| Cell::new("…")).collect();
            table.add_row(dots);
            add_dots = false;
        }

        // if we have reached the second split to be printed, find the correct index to start from
        if processed_rows + num_rows > n_last_rows && needs_split {
            // if it's the first batch, start index is at total_rows - processed_rows - remaining_rows
            let (row_range, rows_to_take) = if processed_rows < n_last_rows {
                let remaining_rows = max_rows - tbl_rows;
                let start = total_rows - processed_rows - remaining_rows;
                (start..num_rows, num_rows - start)
            } else {
                (0..num_rows, num_rows)
            };

            process_batch(
                &mut table,
                opts,
                batch,
                row_range,
                column_ranges.clone(),
                reduce_columns,
            )?;
            tbl_rows += rows_to_take;
        }

        processed_rows += num_rows;
    }
    process_footer(&mut table, total_rows, max_rows, n_tbl_cols)?;
    Ok(table)
}

fn make_str_val(v: &str, truncate: usize) -> String {
    let v_trunc = &v[..v
        .char_indices()
        .take(truncate)
        .last()
        .map(|(i, c)| i + c.len_utf8())
        .unwrap_or(0)];
    if v == v_trunc {
        v.to_string()
    } else {
        format!("{v_trunc}…")
    }
}

fn fmt_timeunit(tu: &TimeUnit) -> String {
    match tu {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "µs",
        TimeUnit::Nanosecond => "ns",
    }
    .to_string()
}

fn fmt_timezone(tz: &Option<Arc<str>>) -> String {
    match tz {
        Some(tz) => tz.as_ref().to_string(),
        None => "UTC".to_string(),
    }
}

fn field_to_str(f: &Field, str_truncate: usize) -> String {
    let column_name = make_str_val(f.name(), str_truncate);

    let column_data_type = match f.data_type() {
        DataType::Timestamp(tu, tz) => {
            format!("\nTimestamp[{}, {}]", fmt_timeunit(tu), fmt_timezone(tz))
        }
        dtype => format!("\n{}", dtype),
    };

    let separator = "\n──";

    format!("{column_name}{separator}{column_data_type}")
}

fn process_header(
    table: &mut Table,
    batch: &RecordBatch,
    column_ranges: (Range<usize>, Range<usize>),
    reduce_columns: bool,
    str_truncate: usize,
) -> Result<(), ArrowError> {
    let schema = batch.schema();
    let fields = schema.fields();
    let first_range = &fields[column_ranges.0];
    let second_range = &fields[column_ranges.1];
    let mut headers: Vec<_> = first_range
        .iter()
        .map(|f| field_to_str(f, str_truncate))
        .collect();
    if reduce_columns {
        headers.push("…".to_string());
    }

    headers.extend(second_range.iter().map(|f| field_to_str(f, str_truncate)));
    table.set_header(headers);
    Ok(())
}

fn process_footer(
    table: &mut Table,
    num_rows: usize,
    max_rows: usize,
    num_columns: usize,
) -> Result<(), ArrowError> {
    let row_info = if num_rows == max_rows {
        return Ok(());
    } else {
        format!("{} rows ({} shown)", num_rows, max_rows)
    };
    let dots: Vec<_> = (0..num_columns).map(|_| Cell::new("───")).collect();
    table.add_row(dots);
    let mut footer = vec![Cell::new(row_info)];
    footer.extend((1..num_columns).map(|_| Cell::new("")));
    table.add_row(footer);
    Ok(())
}

fn process_batch(
    table: &mut Table,
    opts: &FormatOptions,
    batch: &RecordBatch,
    rows: std::ops::Range<usize>,
    column_ranges: (Range<usize>, Range<usize>),
    reduce_columns: bool,
) -> Result<(), ArrowError> {
    let columns = batch.columns();
    let first_range = &columns[column_ranges.0];
    let second_range = &columns[column_ranges.1];

    let first_batch = first_range
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), opts))
        .collect::<Result<Vec<_>, ArrowError>>()?;

    let second_batch = second_range
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), opts))
        .collect::<Result<Vec<_>, ArrowError>>()?;

    for row in rows {
        let mut cells: Vec<_> = first_batch
            .iter()
            .map(|f| Cell::new(f.value(row)))
            .collect();
        if reduce_columns {
            cells.push(Cell::new("…"));
        }
        cells.extend(second_batch.iter().map(|f| Cell::new(f.value(row))));
        table.add_row(cells);
    }

    Ok(())
}
