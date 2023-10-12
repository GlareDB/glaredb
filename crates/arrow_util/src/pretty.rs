use comfy_table::{Cell, CellAlignment, ColumnConstraint, ContentArrangement, Table};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use once_cell::sync::Lazy;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use textwrap::{core::display_width, fill_inplace, wrap};

const DEFAULT_PRESET: &str = "││──╞═╪╡│    ┬┴┌┐└┘";
const DEFAULT_MAX_ROWS: usize = 20;

/// How many values to use for the avg width calculation.
const NUM_VALS_FOR_AVG: usize = 10;

static TABLE_FORMAT_OPTS: Lazy<FormatOptions> = Lazy::new(|| {
    FormatOptions::default()
        .with_display_error(false)
        .with_null("NULL")
});

/// Pretty format record batches.
pub fn pretty_format_batches(
    schema: &Schema,
    batches: &[RecordBatch],
    max_width: Option<usize>,
    max_rows: Option<usize>,
) -> Result<impl fmt::Display, ArrowError> {
    PrettyTable::try_new(schema, batches, max_width, max_rows)
}

#[derive(Debug)]
struct PrettyTable {
    table: Table,
    footer: Option<TableFooter>,
}

#[derive(Debug, Clone, Copy)]
struct TableFooter {
    num_rows_processed: usize,
    max_rows: usize,
}

impl fmt::Display for PrettyTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.table)?;
        if let Some(footer) = self.footer {
            write!(
                f,
                "\n {} rows ({} shown)",
                footer.num_rows_processed, footer.max_rows
            )?;
        }
        Ok(())
    }
}

impl PrettyTable {
    fn try_new(
        schema: &Schema,
        batches: &[RecordBatch],
        max_width: Option<usize>,
        max_rows: Option<usize>,
    ) -> Result<Self, ArrowError> {
        let mut table = Table::new();
        table.load_preset(DEFAULT_PRESET);
        table.set_content_arrangement(ContentArrangement::Dynamic);

        // Create column headers from the schema.
        let col_headers: Vec<_> = schema
            .fields
            .iter()
            .map(|f| ColumnHeader::from_field(f))
            .collect();

        // Try to get some of the values from the first batch. This will be used
        // to help determine the size of the columns.
        let first_vals: Vec<_> = match batches.first() {
            Some(batch) => batch
                .slice(0, std::cmp::min(NUM_VALS_FOR_AVG, batch.num_rows()))
                .columns()
                .iter()
                .map(|col| ColumnValues::try_new_from_array(col, None))
                .collect::<Result<_, _>>()?,
            None => vec![ColumnValues::default(); col_headers.len()],
        };

        let max_width = max_width.or(table.width().map(|v| v as usize));
        let format =
            TableFormat::from_headers_and_sample_vals(&col_headers, &first_vals, max_width);

        // Filter out headers for columns that we'll be hiding. This will also
        // ensure the heading text is the correct width as well.
        let col_headers: Vec<_> = col_headers
            .into_iter()
            .enumerate()
            .filter_map(|(idx, mut h)| {
                if format.is_elided[idx] {
                    None
                } else {
                    if let Some(width) = format.widths[idx] {
                        h.truncate(width);
                    }
                    Some(h)
                }
            })
            .collect();

        let has_ellided = format.has_ellided();

        let mut header_vals: Vec<_> = col_headers.iter().map(|h| h.formatted_string()).collect();
        if has_ellided {
            header_vals.insert(elide_index(&header_vals), "…".to_string());
        }
        table.set_header(header_vals);

        // We're manually truncating/wrapping our values, and so the content should
        // always be the correct size.
        table.set_constraints(
            std::iter::repeat(ColumnConstraint::ContentWidth).take(col_headers.len() + 1),
        );

        // Set column alignments.
        let mut alignments: Vec<_> = col_headers.iter().map(|h| h.alignment).collect();
        if has_ellided {
            alignments.insert(elide_index(&alignments), CellAlignment::Left);
        }
        for (col, alignment) in table.column_iter_mut().zip(alignments.into_iter()) {
            col.set_cell_alignment(alignment);
        }

        // Print batches.

        let max_rows = max_rows.unwrap_or(DEFAULT_MAX_ROWS);
        let total_rows = batches.iter().fold(0, |acc, b| acc + b.num_rows());
        let (row_split_idx, needs_split, mut add_dots) = if max_rows >= total_rows {
            (total_rows, false, false)
        } else {
            (max_rows / 2, true, true)
        };

        let mut tbl_rows = 0;
        let mut processed_rows = 0;
        let n_last_rows = total_rows - row_split_idx;
        for batch in batches {
            let num_rows = batch.num_rows();
            // if the batch is smaller than the row split upper bound, we can just
            // process it in one go otherwise, we need to process them in 2 halves.
            // The first half is from 0..row_split, the second half is from
            // (max_rows - (row_split +1))..max_rows if we split it, we add a
            // placeholder row in the middle
            if tbl_rows < row_split_idx {
                let remaining_rows = row_split_idx - tbl_rows;
                let rows_to_take = remaining_rows.min(num_rows);
                process_batch(&mut table, &format, batch, 0..rows_to_take)?;
                tbl_rows += rows_to_take;
            }

            if tbl_rows >= row_split_idx && add_dots {
                // Add continuation
                let mut dots: Vec<_> = (0..col_headers.len()).map(|_| Cell::new("…")).collect();
                if has_ellided {
                    dots.push(Cell::new("…"));
                }
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

                process_batch(&mut table, &format, batch, row_range)?;
                tbl_rows += rows_to_take;
            }

            processed_rows += num_rows;
        }

        let footer = if processed_rows > max_rows {
            Some(TableFooter {
                num_rows_processed: processed_rows,
                max_rows,
            })
        } else {
            None
        };

        Ok(PrettyTable { table, footer })
    }
}

/// Truncate or wrap a string as to not exceed `width` (including the possible
/// overflow character).
fn truncate_or_wrap_string(s: &mut String, width: usize) {
    // TODO: Handle zero width whitespace and other weird whitespace
    // characters...

    // This is mostly a workaround to ensure we show the entirety of explains.
    // This also means that user data with new lines will be shown in full, but
    // it will be wrapped appropriately.
    if s.contains('\n') {
        const LARGE_STRING_CUTOFF: usize = 2048;
        // Don't do anything fancy for "large" strings to avoid reallocation.
        if s.len() >= LARGE_STRING_CUTOFF {
            fill_inplace(s, width);
            return;
        }

        let lines = wrap(s, width - 1); // Include space for line break arrow.

        let new_s = lines.join("↵\n");
        *s = new_s;

        return;
    }

    if display_width(s) <= width {
        return;
    }

    // Find char boundary to split on.
    for i in 1..s.len() - 1 {
        if s.is_char_boundary(width - i) {
            s.truncate(width - i);
            s.push('…');
            return;
        }
    }

    // I don't believe it's possible for us to actually get here...
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

/// Header for a single column.
struct ColumnHeader {
    name: String,
    data_type: String,
    alignment: CellAlignment,
}

pub fn fmt_dtype(dtype: &DataType) -> String {
    match dtype {
        DataType::Timestamp(tu, tz) => {
            format!("Timestamp<{}, {}>", fmt_timeunit(tu), fmt_timezone(tz))
        }
        DataType::List(fld) | DataType::LargeList(fld) => {
            format!("List<{}>", fmt_dtype(fld.data_type()))
        }
        DataType::Struct(flds) => flds
            .iter()
            .map(|f| format!("{}: {}", f.name(), fmt_dtype(f.data_type())))
            .collect::<Vec<_>>()
            .join(", "),
        DataType::Decimal128(_, _) => "Decimal128".to_string(),
        DataType::Decimal256(_, _) => "Decimal256".to_string(),
        dtype => format!("{}", dtype),
    }
}

impl ColumnHeader {
    fn from_field(f: &Field) -> Self {
        let alignment = if f.data_type().is_numeric() {
            CellAlignment::Right
        } else {
            CellAlignment::Left
        };

        ColumnHeader {
            name: f.name().clone(),
            data_type: fmt_dtype(f.data_type()),
            alignment,
        }
    }

    /// Returns the width in characters that this column header will take up.
    fn width(&self) -> usize {
        if self.name.len() > self.data_type.len() {
            self.name.len()
        } else {
            self.data_type.len()
        }
    }

    fn truncate(&mut self, width: usize) {
        truncate_or_wrap_string(&mut self.name, width);
        truncate_or_wrap_string(&mut self.data_type, width);
    }

    fn formatted_string(&self) -> String {
        const SEP: &str = "\n──\n";
        format!("{}{SEP}{}", self.name, self.data_type)
    }
}

/// Format information for the table
#[derive(Debug, Clone)]
struct TableFormat {
    /// Widths we should use for each column.
    ///
    /// This will include sizes for _all_ columns, including ones that are
    /// ellided.
    widths: Vec<Option<usize>>,
    /// Whether or not the column is ellided.
    is_elided: Vec<bool>,
}

#[derive(Debug, Clone, Copy)]
struct HeaderWidth {
    idx: usize,
    width: usize,
}

impl TableFormat {
    /// Create a table format from the headers and some sample values.
    fn from_headers_and_sample_vals(
        headers: &[ColumnHeader],
        batch_vals: &[ColumnValues],
        max_width: Option<usize>,
    ) -> Self {
        let max_width = match max_width {
            Some(w) => w,
            None => {
                // No max width specified, no need to do fancy table truncation.
                return TableFormat {
                    widths: vec![None; headers.len()],
                    is_elided: vec![false; headers.len()],
                };
            }
        };

        let mut header_widths: Vec<HeaderWidth> = headers
            .iter()
            .enumerate()
            .map(|(idx, h)| HeaderWidth {
                idx,
                width: h.width(),
            })
            .collect();

        const MIN_COLS: usize = 3;

        let mut has_ellided = false;

        // Ellide columns based on the column name/type width. This is info we
        // typically want to display in full.
        loop {
            let total_width: usize = header_widths.iter().fold(0, |acc, h| acc + h.width);
            if total_width < Self::compute_usable_width(max_width, header_widths.len(), has_ellided)
            {
                break;
            }

            if header_widths.len() <= MIN_COLS {
                let usable =
                    Self::compute_usable_width(max_width, header_widths.len(), has_ellided);

                let per_col_width = usable / header_widths.len();

                header_widths
                    .iter_mut()
                    .for_each(|h| h.width = per_col_width);
                break;
            }

            let mid = header_widths.len() / 2;
            header_widths.remove(mid);
            has_ellided = true;
        }

        let stats: Vec<_> = batch_vals.iter().map(|v| v.size_stats()).collect();

        // Grow based on column average.
        Self::grow_using_stats(&mut header_widths, &stats, max_width, has_ellided, |stat| {
            stat.avg
        });
        // Grow based on column max.
        Self::grow_using_stats(&mut header_widths, &stats, max_width, has_ellided, |stat| {
            stat.max
        });

        let mut format = TableFormat {
            widths: vec![None; headers.len()],
            is_elided: vec![true; headers.len()],
        };
        for header in header_widths {
            format.widths[header.idx] = Some(header.width);
            format.is_elided[header.idx] = false;
        }

        format
    }

    /// Grow headers widths based on some stats.
    fn grow_using_stats(
        header_widths: &mut [HeaderWidth],
        stats: &[ColumnWidthSizeStats],
        max_width: usize,
        has_ellided: bool,
        stat_fn: impl Fn(&ColumnWidthSizeStats) -> usize,
    ) {
        let mut total_width: usize = header_widths.iter().fold(0, |acc, h| acc + h.width);
        let num_cols = header_widths.len();
        for (header, stat) in header_widths.iter_mut().zip(stats.iter()) {
            let stat_val = stat_fn(stat);

            let rem = Self::compute_usable_width(max_width, num_cols, has_ellided) - total_width;
            if rem == 0 {
                // No more space to give.
                return;
            }

            if stat_val <= header.width {
                // We're already a good size.
                continue;
            }

            let grow_amount = stat_val - header.width;
            let grow_amount = std::cmp::min(grow_amount, rem);

            header.width += grow_amount;
            total_width += grow_amount;
        }
    }

    // Compute the width in characters that can be used by column values.
    const fn compute_usable_width(max_width: usize, num_cols: usize, has_ellided: bool) -> usize {
        // For each column, subtract left and right padding, and the leading
        // border character. The extra '- 1' is for the last border.
        //
        // Note for small max widths and a large number of columns, there's a
        // chance to underflow. So we should just clamp to '0' since there's no
        // usable space.
        let column_padding = num_cols * 3;
        let mut usable = max_width.saturating_sub(column_padding).saturating_sub(1);
        if has_ellided {
            // Make sure we include the space taken up by the ... column.
            //
            // dots: 1 char
            // leading border: 1 char
            // padding: 2 chars
            usable = usable.saturating_sub(4);
        }
        usable
    }

    fn has_ellided(&self) -> bool {
        self.is_elided.iter().any(|b| *b)
    }
}

#[derive(Debug, Default, Clone)]
struct ColumnValues {
    vals: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct ColumnWidthSizeStats {
    avg: usize,
    _min: usize, // Unused, but could possibly use to shrink columns in the future.
    max: usize,
}

impl ColumnValues {
    fn try_new_from_array(col: &dyn Array, trunc: Option<usize>) -> Result<Self, ArrowError> {
        let formatter = ArrayFormatter::try_new(col, &TABLE_FORMAT_OPTS)?;
        let vals = (0..col.len())
            .map(|idx| {
                let mut s = formatter.value(idx).try_to_string()?;
                if let Some(trunc) = trunc {
                    truncate_or_wrap_string(&mut s, trunc);
                }
                Ok(s)
            })
            .collect::<Result<Vec<_>, ArrowError>>()?;
        Ok(ColumnValues { vals })
    }

    /// Compute various size stats on the column. Used when determining a good
    /// width for the column.
    fn size_stats(&self) -> ColumnWidthSizeStats {
        let mut avg = 0.0;
        let mut min = self
            .vals
            .first()
            .map(|v| display_width(v))
            .unwrap_or_default();
        let mut max = 0;
        for (idx, val) in self.vals.iter().enumerate() {
            let width = display_width(val);

            avg += (width as f64 - avg) / ((idx + 1) as f64);

            if width < min {
                min = width;
            }
            if width > max {
                max = width;
            }
        }

        ColumnWidthSizeStats {
            avg: avg as usize,
            _min: min,
            max,
        }
    }
}

fn process_batch(
    table: &mut Table,
    format: &TableFormat,
    batch: &RecordBatch,
    rows: Range<usize>,
) -> Result<(), ArrowError> {
    if rows.is_empty() {
        return Ok(());
    }

    // Avoid trying to process the entire batch.
    let batch = batch.slice(rows.start, rows.len());

    let mut col_vals = Vec::new();
    for (idx, col) in batch.columns().iter().enumerate() {
        if format.is_elided[idx] {
            continue;
        }
        let vals = ColumnValues::try_new_from_array(col, format.widths[idx])?;
        col_vals.push(vals);
    }

    // Note this is starting from the beginning of the sliced batch.
    for batch_idx in 0..batch.num_rows() {
        let mut cells = Vec::with_capacity(col_vals.len());
        for col in col_vals.iter_mut() {
            cells.push(std::mem::take(col.vals.get_mut(batch_idx).unwrap()));
        }

        if format.has_ellided() {
            cells.insert(elide_index(&cells), "…".to_string());
        }

        table.add_row(cells);
    }

    Ok(())
}

/// Return the index where a '...' should be inserted.
const fn elide_index<T>(v: &[T]) -> usize {
    let mid = v.len() / 2;
    if v.len() % 2 == 0 {
        mid
    } else {
        mid + 1
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Int32Array, StringArray};

    use super::*;

    #[test]
    fn test_truncate_string() {
        #[derive(Debug)]
        struct TestCase {
            input: &'static str,
            truncate: usize,
            expected: &'static str,
        }

        let test_cases = &[
            TestCase {
                input: "test",
                truncate: 32,
                expected: "test",
            },
            TestCase {
                input: "test",
                truncate: 4,
                expected: "test",
            },
            TestCase {
                input: "test",
                truncate: 3,
                expected: "te…",
            },
            TestCase {
                input: "hello\nworld",
                truncate: 8,
                expected: "hello↵\nworld",
            },
            TestCase {
                input: "hello\nworld",
                truncate: 3,
                expected: "he↵\nll↵\no↵\nwo↵\nrl↵\nd",
            },
        ];

        for tc in test_cases {
            let mut s = tc.input.to_string();
            truncate_or_wrap_string(&mut s, tc.truncate);
            assert_eq!(tc.expected, &s, "test case: {tc:?}");
        }
    }

    /// Assert equality and place both values in the assert message for easier
    /// of test failures.
    fn assert_eq_print<S: AsRef<str>>(expected: S, got: S) {
        let expected = expected.as_ref();
        let got = got.as_ref();
        assert_eq!(expected, got, "\nexpected:\n{expected}\ngot:\n{got}")
    }

    #[test]
    fn simple_no_batches() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let table = pretty_format_batches(&schema, &[], None, None).unwrap();
        let expected = [
            "┌───────┬──────┐",
            "│     a │ b    │",
            "│    ── │ ──   │",
            "│ Int64 │ Utf8 │",
            "╞═══════╪══════╡",
            "└───────┴──────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn simple_single_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    None,
                    Some("d"),
                ])),
                Arc::new(Int32Array::from(vec![Some(1), None, Some(10), Some(100)])),
            ],
        )
        .unwrap();

        let table = pretty_format_batches(&schema, &[batch], None, None).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │     b │",
            "│ ──   │    ── │",
            "│ Utf8 │ Int32 │",
            "╞══════╪═══════╡",
            "│ a    │     1 │",
            "│ b    │  NULL │",
            "│ NULL │    10 │",
            "│ d    │   100 │",
            "└──────┴───────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn multiple_small_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(Int32Array::from(vec![Some(1)])),
            ],
        )
        .unwrap();

        let batches = vec![batch; 4];

        let table = pretty_format_batches(&schema, &batches, None, None).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │     b │",
            "│ ──   │    ── │",
            "│ Utf8 │ Int32 │",
            "╞══════╪═══════╡",
            "│ a    │     1 │",
            "│ a    │     1 │",
            "│ a    │     1 │",
            "│ a    │     1 │",
            "└──────┴───────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let create_batch = |s, n| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some(s)])),
                    Arc::new(Int32Array::from(vec![Some(n)])),
                ],
            )
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 1),
            create_batch("b", 2),
            create_batch("c", 3),
            create_batch("d", 4),
            create_batch("e", 5),
            create_batch("f", 6),
        ];

        let table = pretty_format_batches(&schema, &batches, None, Some(4)).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │     b │",
            "│ ──   │    ── │",
            "│ Utf8 │ Int32 │",
            "╞══════╪═══════╡",
            "│ a    │     1 │",
            "│ b    │     2 │",
            "│ …    │     … │",
            "│ e    │     5 │",
            "│ f    │     6 │",
            "└──────┴───────┘",
            " 6 rows (4 shown)",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn large_batch_with_max_rows() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        let a_vals: Vec<_> = (0..10).map(|v| Some(v.to_string())).collect();
        let b_vals: Vec<_> = (0..10).map(Some).collect();

        let batches = vec![RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(a_vals)),
                Arc::new(Int32Array::from(b_vals)),
            ],
        )
        .unwrap()];

        let table = pretty_format_batches(&schema, &batches, None, Some(4)).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │     b │",
            "│ ──   │    ── │",
            "│ Utf8 │ Int32 │",
            "╞══════╪═══════╡",
            "│ 0    │     0 │",
            "│ 1    │     1 │",
            "│ …    │     … │",
            "│ 8    │     8 │",
            "│ 9    │     9 │",
            "└──────┴───────┘",
            " 10 rows (4 shown)",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_value() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));

        let create_batch = |a, b, c, d| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some(a)])),
                    Arc::new(Int32Array::from(vec![Some(b)])),
                    Arc::new(StringArray::from(vec![Some(c)])),
                    Arc::new(StringArray::from(vec![Some(d)])),
                ],
            )
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 1, "c", "d"),
            create_batch("a", 2, "ccccc", "d"),
            create_batch("a", 3, "cccccccccc", "d"),
            create_batch("a", 4, "cccccccccccccccccc", "d"),
        ];

        let table = pretty_format_batches(&schema, &batches, Some(40), None).unwrap();

        // Note this doesn't grow great since we're only computing column stats
        // on the first batch. The next test shows the growth behavior better by
        // having the first batch have the longest value.
        let expected = [
            "┌──────┬───────┬──────┬──────┐",
            "│ a    │     b │ c    │ d    │",
            "│ ──   │    ── │ ──   │ ──   │",
            "│ Utf8 │ Int32 │ Utf8 │ Utf8 │",
            "╞══════╪═══════╪══════╪══════╡",
            "│ a    │     1 │ c    │ d    │",
            "│ a    │     2 │ ccc… │ d    │",
            "│ a    │     3 │ ccc… │ d    │",
            "│ a    │     4 │ ccc… │ d    │",
            "└──────┴───────┴──────┴──────┘",
        ];

        // I'm just copy pasting output I'm getting. This is here to make sure
        // what's expected is actually correct.
        assert!(display_width(expected[0]) <= 40);

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_value_first() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));

        let create_batch = |a, b, c, d| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some(a)])),
                    Arc::new(Int32Array::from(vec![Some(b)])),
                    Arc::new(StringArray::from(vec![Some(c)])),
                    Arc::new(StringArray::from(vec![Some(d)])),
                ],
            )
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 4, "cccccccccccccccccc", "d"),
            create_batch("a", 3, "cccccccccc", "d"),
            create_batch("a", 2, "ccccc", "d"),
            create_batch("a", 1, "c", "d"),
        ];

        let table = pretty_format_batches(&schema, &batches, Some(40), None).unwrap();

        let expected = [
            "┌──────┬───────┬────────────────┬──────┐",
            "│ a    │     b │ c              │ d    │",
            "│ ──   │    ── │ ──             │ ──   │",
            "│ Utf8 │ Int32 │ Utf8           │ Utf8 │",
            "╞══════╪═══════╪════════════════╪══════╡",
            "│ a    │     4 │ ccccccccccccc… │ d    │",
            "│ a    │     3 │ cccccccccc     │ d    │",
            "│ a    │     2 │ ccccc          │ d    │",
            "│ a    │     1 │ c              │ d    │",
            "└──────┴───────┴────────────────┴──────┘",
        ];

        assert!(display_width(expected[0]) <= 40); // See above test.

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_column_name() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("thisisasomewhatlongcolumn", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
        ]));

        let create_batch = |a, b, c| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some(a)])),
                    Arc::new(Int32Array::from(vec![Some(b)])),
                    Arc::new(StringArray::from(vec![Some(c)])),
                ],
            )
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 4, "cccccccccccccccccc"),
            create_batch("a", 3, "cccccccccc"),
            create_batch("a", 2, "ccccc"),
            create_batch("a", 1, "c"),
        ];

        let table = pretty_format_batches(&schema, &batches, Some(40), None).unwrap();

        let expected = [
            "┌──────┬────────────┬────────────┐",
            "│ a    │ thisisaso… │ c          │",
            "│ ──   │         ── │ ──         │",
            "│ Utf8 │      Int32 │ Utf8       │",
            "╞══════╪════════════╪════════════╡",
            "│ a    │          4 │ ccccccccc… │",
            "│ a    │          3 │ cccccccccc │",
            "│ a    │          2 │ ccccc      │",
            "│ a    │          1 │ c          │",
            "└──────┴────────────┴────────────┘",
        ];
        assert!(display_width(expected[0]) <= 40);

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_column_name_even_num_cols() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("thisisasomewhatlongcolumn", DataType::Int32, true),
            Field::new("c", DataType::Utf8, true),
            Field::new("d", DataType::Utf8, true),
        ]));

        let create_batch = |a, b, c, d| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec![Some(a)])),
                    Arc::new(Int32Array::from(vec![Some(b)])),
                    Arc::new(StringArray::from(vec![Some(c)])),
                    Arc::new(StringArray::from(vec![Some(d)])),
                ],
            )
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 4, "cccccccccccccccccc", "d"),
            create_batch("a", 3, "cccccccccc", "d"),
            create_batch("a", 2, "ccccc", "d"),
            create_batch("a", 1, "c", "d"),
        ];

        let table = pretty_format_batches(&schema, &batches, Some(40), None).unwrap();

        let expected = [
            "┌──────┬──────────┬───┬──────┐",
            "│ a    │ thisisa… │ … │ d    │",
            "│ ──   │       ── │   │ ──   │",
            "│ Utf8 │    Int32 │   │ Utf8 │",
            "╞══════╪══════════╪═══╪══════╡",
            "│ a    │        4 │ … │ d    │",
            "│ a    │        3 │ … │ d    │",
            "│ a    │        2 │ … │ d    │",
            "│ a    │        1 │ … │ d    │",
            "└──────┴──────────┴───┴──────┘",
        ];

        assert!(display_width(expected[0]) <= 40);

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn many_cols_small_max_width() {
        // https://github.com/GlareDB/glaredb/issues/1790

        let fields: Vec<_> = (0..30)
            .map(|i| Field::new(i.to_string(), DataType::Int8, true))
            .collect();

        let schema = Arc::new(Schema::new(fields));

        let table = pretty_format_batches(&schema, &[], Some(40), None).unwrap();

        let expected = [
            "┌──────┬──────┬───┬──────┬──────┐",
            "│    0 │    1 │ … │   28 │   29 │",
            "│   ── │   ── │   │   ── │   ── │",
            "│ Int8 │ Int8 │   │ Int8 │ Int8 │",
            "╞══════╪══════╪═══╪══════╪══════╡",
            "└──────┴──────┴───┴──────┴──────┘",
        ];

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn multiple_batches_with_slicing() {
        // https://github.com/GlareDB/glaredb/pull/1788
        //
        // Make sure batch slicing is correct.
        //
        // - 3 batches, 2 records each
        // - max rows is 2
        // - first record of first batch and last record of last batch should be printed

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Int32, true),
        ]));

        // First record should be printed.
        let first = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("1"), Some("2")])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
            ],
        )
        .unwrap();

        // Nothing in this batch should be printed.
        let middle = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("3"), Some("4")])),
                Arc::new(Int32Array::from(vec![Some(3), Some(4)])),
            ],
        )
        .unwrap();

        // Last record should be printed.
        let last = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec![Some("5"), Some("6")])),
                Arc::new(Int32Array::from(vec![Some(5), Some(6)])),
            ],
        )
        .unwrap();

        let table = pretty_format_batches(&schema, &[first, middle, last], None, Some(2)).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │     b │",
            "│ ──   │    ── │",
            "│ Utf8 │ Int32 │",
            "╞══════╪═══════╡",
            "│ 1    │     1 │",
            "│ …    │     … │",
            "│ 6    │     6 │",
            "└──────┴───────┘",
            " 6 rows (2 shown)",
        ];

        assert_eq_print(expected.join("\n"), table.to_string())
    }
}
