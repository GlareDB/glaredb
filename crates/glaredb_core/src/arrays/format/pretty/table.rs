use std::borrow::Borrow;
use std::collections::HashMap;
use std::fmt::{self, Write as _};
use std::ops::Range;

use glaredb_error::Result;
use textwrap::core::display_width;
use textwrap::{fill_inplace, wrap};

use super::components::TableComponents;
use super::display::{Alignment, PrettyFooter, PrettyHeader, PrettyValues, table_width};
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::field::ColumnSchema;
use crate::arrays::format::{FormatOptions, Formatter};

/// How many values to use for the avg width calculation.
const NUM_VALS_FOR_AVG: usize = 30;

/// Default number of rows to display.
const DEFAULT_MAX_ROWS: usize = 50;

const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

#[derive(Debug)]
pub struct PrettyTable {
    header: PrettyHeader,
    head: Vec<PrettyValues>,
    tail: Vec<PrettyValues>,
    footer: PrettyFooter,
}

impl PrettyTable {
    /// Try to create a new pretty-formatted table.
    pub fn try_new<B: Borrow<Batch>>(
        schema: &ColumnSchema,
        batches: &[B],
        max_width: usize,
        max_rows: Option<usize>,
        components: &'static TableComponents,
    ) -> Result<Self> {
        if schema.fields.is_empty() {
            let header = ColumnValues::try_new_arbitrary_header(
                "Query success",
                "No columns returned",
                None,
            )?;
            let widths = vec![header.value(1).len()];
            return Ok(PrettyTable {
                header: PrettyHeader::new(components, widths.clone(), vec![header], false),
                head: Vec::new(),
                tail: Vec::new(),
                footer: PrettyFooter {
                    components,
                    content: String::new(),
                    column_widths: widths,
                },
            });
        }

        let headers = schema
            .fields
            .iter()
            .map(|field| {
                ColumnValues::try_from_column_name_and_type(&field.name, &field.datatype, None)
            })
            .collect::<Result<Vec<_>>>()?;

        let col_alignments: Vec<_> = schema
            .fields
            .iter()
            .map(|f| {
                if f.datatype.is_numeric() {
                    Alignment::Right
                } else {
                    Alignment::Left
                }
            })
            .collect();

        // Get column width stats from first sample of rows. We'll use these to
        // determine best widths to use per column.
        let samples = (0..schema.fields.len())
            .map(|col_idx| ColumnWidthSizeStats::from_batches(batches, col_idx, NUM_VALS_FOR_AVG))
            .collect::<Result<Vec<_>>>()?;

        let format = TableFormat::from_headers_and_samples(&headers, &samples, max_width);

        // Filter out headers for columns that we'll be hiding. This will also
        // ensure the heading text is the correct width as well.
        let mut headers = schema
            .fields
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if format.is_elided[idx] {
                    None
                } else {
                    Some(ColumnValues::try_from_column_name_and_type(
                        &field.name,
                        &field.datatype,
                        format.widths[idx],
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        if format.has_ellided() {
            headers.insert(elide_index(&headers), ColumnValues::elided_column(false, 2));
        }

        let mut col_alignments: Vec<_> = col_alignments
            .into_iter()
            .enumerate()
            .filter_map(|(idx, alignment)| {
                if format.is_elided[idx] {
                    None
                } else {
                    Some(alignment)
                }
            })
            .collect();
        if format.has_ellided() {
            col_alignments.insert(elide_index(&col_alignments), Alignment::Left);
        }

        let mut column_widths: Vec<_> = format
            .widths
            .iter()
            .enumerate()
            .filter_map(|(idx, width)| {
                if format.is_elided[idx] {
                    None
                } else {
                    Some(width.unwrap_or(20))
                }
            })
            .collect();
        if format.has_ellided() {
            column_widths.insert(elide_index(&column_widths), 1);
        }

        let total_rows: usize = batches.iter().map(|b| b.borrow().num_rows()).sum();
        let max_rows = max_rows.unwrap_or(DEFAULT_MAX_ROWS);

        let (mut head_rows, mut tail_rows) = if total_rows > max_rows {
            (max_rows / 2, max_rows / 2)
        } else {
            (max_rows, 0)
        };

        if max_rows % 2 != 0 {
            head_rows += 1;
        }

        // Generate footer content first so we can adjust columns widths to make
        // sure it fits prior to cloning the widths.
        let footer_content = if total_rows == 0 {
            "No rows".to_string()
        } else if total_rows > max_rows {
            format!("{total_rows} rows, {max_rows} shown")
        } else {
            String::new()
        };

        // If the table's too small to fit our footer, widen it by distributing
        // widths to columns until it's wide enough.
        if table_width(&column_widths) - 4 < footer_content.len() {
            let mut need = footer_content.len() - (table_width(&column_widths) - 4);
            let mut width_idx = 0;
            while need > 0 {
                column_widths[width_idx] += 1;
                need -= 1;
                width_idx = (width_idx + 1) % column_widths.len();
            }
        }

        // Get head rows.
        let mut head = Vec::new();
        for batch in batches {
            if head_rows == 0 {
                break;
            }

            let (vals, num_rows) =
                Self::column_values_for_batch(batch.borrow(), &format, 0..head_rows)?;
            head.push(PrettyValues::new(
                components,
                col_alignments.clone(),
                column_widths.clone(),
                vals,
            ));
            head_rows -= num_rows;
        }

        // Get tail rows.
        let mut tail = Vec::new();
        for batch in batches.iter().rev() {
            if tail_rows == 0 {
                break;
            }

            let num_rows = batch.borrow().num_rows();
            let range = if tail_rows >= num_rows {
                0..num_rows
            } else {
                (num_rows - tail_rows)..num_rows
            };
            let (vals, num_rows) = Self::column_values_for_batch(batch.borrow(), &format, range)?;
            tail.push(PrettyValues::new(
                components,
                col_alignments.clone(),
                column_widths.clone(),
                vals,
            ));
            tail_rows -= num_rows;
        }

        // Add dots
        if !tail.is_empty() {
            let dot_cols: Vec<_> = (0..headers.len())
                .map(|_| ColumnValues::elided_column(true, 1))
                .collect();
            tail.push(PrettyValues::new(
                components,
                col_alignments.clone(),
                column_widths.clone(),
                dot_cols,
            ));
        }

        // Since we worked backwards when getting the tail rows.
        tail.reverse();

        Ok(PrettyTable {
            header: PrettyHeader::new(components, column_widths.clone(), headers, !head.is_empty()),
            head,
            tail,
            footer: PrettyFooter {
                components,
                column_widths,
                content: footer_content,
            },
        })
    }

    fn column_values_for_batch(
        batch: &Batch,
        format: &TableFormat,
        range: Range<usize>,
    ) -> Result<(Vec<ColumnValues>, usize)> {
        let mut vals = batch
            .arrays()
            .iter()
            .enumerate()
            .filter_map(|(idx, c)| {
                if format.is_elided[idx] {
                    None
                } else {
                    Some(ColumnValues::try_from_array(
                        c,
                        Some(range.clone()),
                        batch.num_rows(),
                        format.widths[idx],
                    ))
                }
            })
            .collect::<Result<Vec<_>>>()?;
        let num_rows = vals.first().map(|v| v.num_values()).unwrap_or(0);

        if format.has_ellided() {
            vals.insert(
                elide_index(&vals),
                ColumnValues::elided_column(true, num_rows),
            )
        }

        Ok((vals, num_rows))
    }
}

impl fmt::Display for PrettyTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.header.fmt(f)?;
        for val in &self.head {
            val.fmt(f)?;
        }
        for val in &self.tail {
            val.fmt(f)?;
        }
        self.footer.fmt(f)?;

        Ok(())
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeaderWidth {
    idx: usize,
    width: usize,
}

impl TableFormat {
    /// Create an appropriate table format based on the header values and
    /// samples generated from a batch.
    fn from_headers_and_samples(
        headers: &[ColumnValues],
        samples: &[ColumnWidthSizeStats],
        max_width: usize,
    ) -> Self {
        let mut header_widths: Vec<_> = headers
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

        // Grow based on column average.
        Self::grow_using_stats(
            &mut header_widths,
            samples,
            max_width,
            has_ellided,
            |stat| stat.avg,
        );
        // Grow based on column max.
        Self::grow_using_stats(
            &mut header_widths,
            samples,
            max_width,
            has_ellided,
            |stat| stat.max,
        );

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ColumnValues {
    /// Buffer containing all string values.
    pub buffer: String,
    /// Indices for slicing the buffer into individual column values.
    ///
    /// The start index is the index of the value in the column. The end index
    /// is the index of the value + 1.
    pub indices: Vec<usize>,
    /// Height for rows that are not a single line line.
    pub row_heights: HashMap<usize, usize>,
}

impl Default for ColumnValues {
    fn default() -> Self {
        ColumnValues {
            buffer: String::new(),
            indices: vec![0],
            row_heights: HashMap::new(),
        }
    }
}

impl ColumnValues {
    /// Create columns values with '…' values.
    ///
    /// If `all` is false, only the first column will have '…', the rest will be
    /// empty strings.
    pub fn elided_column(all: bool, num_vals: usize) -> Self {
        let mut buf = String::from("…");
        let mut indices = vec![0, buf.len()];

        let val = if all { '…' } else { ' ' };

        for _ in 1..num_vals {
            buf.push(val);
            indices.push(buf.len())
        }

        ColumnValues {
            buffer: buf,
            indices,
            row_heights: HashMap::new(),
        }
    }

    pub fn try_new_arbitrary_header(
        top: &str,
        bottom: &str,
        max_width: Option<usize>,
    ) -> Result<Self> {
        let mut buf = top.to_string();
        if let Some(width) = max_width {
            truncate_or_wrap_string(&mut buf, width);
        }

        let mut indices = vec![0, buf.len()];

        let mut bottom = bottom.to_string();
        if let Some(width) = max_width {
            truncate_or_wrap_string(&mut bottom, width);
        }
        write!(buf, "{bottom}")?;
        indices.push(buf.len());

        Ok(ColumnValues {
            buffer: buf,
            indices,
            row_heights: HashMap::new(),
        })
    }

    /// Turn a column name and type into column values.
    pub fn try_from_column_name_and_type(
        name: &str,
        typ: &DataType,
        max_width: Option<usize>,
    ) -> Result<Self> {
        let mut buf = name.to_string();
        if let Some(width) = max_width {
            truncate_or_wrap_string(&mut buf, width);
        }

        let mut indices = vec![0, buf.len()];

        let mut typ = typ.to_string();
        if let Some(width) = max_width {
            truncate_or_wrap_string(&mut typ, width);
        }
        write!(buf, "{typ}")?;
        indices.push(buf.len());

        Ok(ColumnValues {
            buffer: buf,
            indices,
            row_heights: HashMap::new(),
        })
    }

    /// Turn an array into columns values.
    ///
    /// Accepts an optional range for converting only part of the array to
    /// strings.
    ///
    /// If the upper bound in the range exceeds `num_rows`, it'll be clamped to
    /// `num_rows`.
    pub fn try_from_array(
        array: &Array,
        range: Option<Range<usize>>,
        num_rows: usize,
        max_width: Option<usize>,
    ) -> Result<Self> {
        let mut buf = String::new();
        let mut indices = vec![0];
        let mut temp_buf = String::new();

        let range = match range {
            Some(range) => {
                if range.start > num_rows {
                    0..0
                } else if range.end > num_rows {
                    range.start..num_rows
                } else {
                    range
                }
            }
            None => 0..num_rows,
        };

        let mut row_heights = HashMap::new();
        for (value_idx, row_idx) in range.enumerate() {
            temp_buf.clear();
            let scalar = FORMATTER
                .format_array_value(array, row_idx)
                .expect("scalar to exist at index");
            write!(temp_buf, "{scalar}")?;

            if let Some(width) = max_width {
                let num_lines = truncate_or_wrap_string(&mut temp_buf, width);
                if num_lines > 1 {
                    row_heights.insert(value_idx, num_lines);
                }
            }

            buf.push_str(&temp_buf);
            indices.push(buf.len());
        }

        Ok(ColumnValues {
            buffer: buf,
            indices,
            row_heights,
        })
    }

    pub fn value(&self, idx: usize) -> &str {
        let start = self.indices[idx];
        let end = self.indices[idx + 1];
        &self.buffer[start..end]
    }

    /// Get the width that this column will take up.
    pub fn width(&self) -> usize {
        self.iter().map(display_width).max().unwrap_or(0)
    }

    pub fn num_values(&self) -> usize {
        self.indices.len() - 1
    }

    pub fn iter(&self) -> ColumnValuesIter<'_> {
        ColumnValuesIter {
            buffer: &self.buffer,
            indices: &self.indices,
            idx: 0,
        }
    }
}

#[derive(Debug)]
pub(crate) struct ColumnValuesIter<'a> {
    buffer: &'a String,
    indices: &'a Vec<usize>,
    idx: usize,
}

impl<'a> Iterator for ColumnValuesIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        let start = *self.indices.get(self.idx)?;
        let end = *self.indices.get(self.idx + 1)?;

        let s = &self.buffer[start..end];
        self.idx += 1;

        Some(s)
    }
}

#[derive(Debug, Clone, Copy)]
struct ColumnWidthSizeStats {
    avg: usize,
    _min: usize, // Unused, but could possibly use to shrink columns in the future.
    max: usize,
}

impl ColumnWidthSizeStats {
    fn from_batches<B>(batches: &[B], col_idx: usize, max_rows: usize) -> Result<Self>
    where
        B: Borrow<Batch>,
    {
        let row_count: usize = batches.iter().map(|b| b.borrow().num_rows()).sum();
        if row_count == 0 {
            return Ok(ColumnWidthSizeStats {
                avg: 0,
                _min: 0,
                max: 0,
            });
        }

        let mut sum = 0;
        let mut min = usize::MAX;
        let mut max = 0;

        let mut count = 0;
        let mut temp_buf = String::new();

        for batch in batches {
            let rem = max_rows - count;
            if rem == 0 {
                break;
            }
            let row_count = usize::min(rem, batch.borrow().num_rows);

            let col = &batch.borrow().arrays[col_idx];
            for row_idx in 0..row_count {
                // TODO: Duplicated.
                temp_buf.clear();
                let scalar = FORMATTER
                    .format_array_value(col, row_idx)
                    .expect("scalar to exist at index");
                write!(temp_buf, "{scalar}")?;

                sum += temp_buf.len();
                min = usize::min(temp_buf.len(), min);
                max = usize::max(temp_buf.len(), max);
            }

            count += row_count;
        }

        // Bias towards larger average.
        let avg = (sum + 1) / count;

        Ok(ColumnWidthSizeStats {
            avg,
            _min: min,
            max,
        })
    }
}

/// Character to use to denote a hardwrap/softwrap in a string that we display.
const LINEBREAK_CHAR: char = '+';

/// Truncate or wrap a string as to not exceed `width` (including the possible
/// overflow character).
///
/// Returns number of lines in this string.
fn truncate_or_wrap_string(s: &mut String, width: usize) -> usize {
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

            let num_lines = s.as_bytes().iter().filter(|&c| *c == b'\n').count() + 1;

            return num_lines;
        }

        let mut lines = wrap(s, width - 1); // Include space for line break character.
        let num_lines = lines.len();

        // We'll handle formatting the last line separately.
        let last = match lines.pop() {
            Some(last) => last,
            None => return 0, // No lines?
        };

        let mut out_buf = String::new();
        // Format everything but the last line. This will mark line breaks at
        // the end of the line.
        for line in lines {
            writeln!(
                out_buf,
                "{:<width$}{lb}",
                line,
                lb = LINEBREAK_CHAR,
                width = width - 1
            )
            .expect("write to string to never fail");
        }

        // Format the last line. Just a simple write, we don't need to worry
        // about writing newlines.
        write!(out_buf, "{last}").expect("write to string to never fail");

        *s = out_buf;

        return num_lines;
    }

    if display_width(s) <= width {
        return 1;
    }

    // Find char boundary to split on.
    for i in 1..s.len() - 1 {
        if s.is_char_boundary(width - i) {
            s.truncate(width - i);
            s.push('…');
            return 1;
        }
    }

    // I don't believe it's possible for us to actually get here...
    1
}

/// Return the index where a '...' should be inserted.
const fn elide_index<T>(v: &[T]) -> usize {
    let mid = v.len() / 2;
    if v.len() % 2 == 0 { mid } else { mid + 1 }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::arrays::array::selection::Selection;
    use crate::arrays::field::Field;
    use crate::arrays::format::pretty::components::PRETTY_COMPONENTS;
    use crate::buffer::buffer_manager::DefaultBufferManager;
    use crate::generate_batch;
    use crate::util::iter::TryFromExactSizeIterator;

    fn pretty_format_batches(
        schema: &ColumnSchema,
        batches: &[Batch],
        max_width: usize,
        max_rows: Option<usize>,
    ) -> Result<impl fmt::Display + use<>> {
        PrettyTable::try_new(schema, batches, max_width, max_rows, PRETTY_COMPONENTS)
    }

    #[test]
    fn test_truncate_string() {
        #[derive(Debug)]
        struct TestCase {
            input: &'static str,
            truncate: usize,
            expected: &'static str,
            expected_lines: usize,
        }

        let test_cases = &[
            TestCase {
                input: "test",
                truncate: 32,
                expected: "test",
                expected_lines: 1,
            },
            TestCase {
                input: "test",
                truncate: 4,
                expected: "test",
                expected_lines: 1,
            },
            TestCase {
                input: "test",
                truncate: 3,
                expected: "te…",
                expected_lines: 1,
            },
            TestCase {
                input: "hello\nworld",
                truncate: 8,
                expected: "hello  +\nworld",
                expected_lines: 2,
            },
            TestCase {
                input: "hello\nworld",
                truncate: 3,
                expected: "he+\nll+\no +\nwo+\nrl+\nd",
                expected_lines: 6,
            },
            TestCase {
                input: "hello world\n",
                truncate: 16,
                expected: "hello world    +\n",
                expected_lines: 2,
            },
        ];

        for tc in test_cases {
            let mut s = tc.input.to_string();
            let lines = truncate_or_wrap_string(&mut s, tc.truncate);
            assert_eq!(tc.expected, &s, "test case: {tc:?}");
            assert_eq!(tc.expected_lines, lines, "test case: {tc:?}");
        }
    }

    /// Assert equality and place both values in the assert message for easier
    /// of test failures.
    #[track_caller]
    fn assert_eq_print<S: AsRef<str>>(expected: S, got: S) {
        let expected = expected.as_ref();
        let got = got.as_ref();
        assert_eq!(expected, got, "\nexpected:\n{expected}\ngot:\n{got}")
    }

    #[test]
    fn no_batches_with_no_columns() {
        let schema = ColumnSchema::new([]);

        let table = pretty_format_batches(&schema, &[], 80, None).unwrap();

        let expected = [
            "┌─────────────────────┐",
            "│ Query success       │",
            "│ No columns returned │",
            "└─────────────────────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn no_batches_with_columns() {
        let schema = ColumnSchema::new([
            Field::new("a", DataType::int64(), false),
            Field::new("b", DataType::utf8(), false),
        ]);

        let table = pretty_format_batches(&schema, &[], 80, None).unwrap();
        let expected = [
            "┌───────┬──────┐",
            "│ a     │ b    │",
            "│ Int64 │ Utf8 │",
            "├───────┴──────┤",
            "│ No rows      │",
            "└──────────────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn simple_single_batch() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let batch = Batch::from_arrays([
            Array::try_from_iter([Some("a"), Some("b"), None, Some("d")]).unwrap(),
            Array::try_from_iter([Some(1), None, Some(10), Some(100)]).unwrap(),
        ])
        .unwrap();

        let table = pretty_format_batches(&schema, &[batch], 80, None).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │ b     │",
            "│ Utf8 │ Int32 │",
            "├──────┼───────┤",
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
    fn multiline_values() {
        let schema = ColumnSchema::new(vec![
            Field::new("c1", DataType::utf8(), true),
            Field::new("c2", DataType::int32(), true),
            Field::new("c3", DataType::utf8(), true),
        ]);

        let batch = Batch::from_arrays(vec![
            Array::try_from_iter([Some("a\nb"), Some("c"), Some("d")]).unwrap(),
            Array::try_from_iter([Some(1), Some(10), Some(100)]).unwrap(),
            Array::try_from_iter([Some("Mario"), Some("Yoshi"), Some("Luigi\nPeach")]).unwrap(),
        ])
        .unwrap();

        let table = pretty_format_batches(&schema, &[batch], 80, None).unwrap();

        let expected = [
            "┌──────┬───────┬─────────────┐",
            "│ c1   │ c2    │ c3          │",
            "│ Utf8 │ Int32 │ Utf8        │",
            "├──────┼───────┼─────────────┤",
            "│ a  + │     1 │ Mario       │",
            "│ b    │       │             │",
            "│ c    │    10 │ Yoshi       │",
            "│ d    │   100 │ Luigi     + │",
            "│      │       │ Peach       │",
            "└──────┴───────┴─────────────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn multiple_small_batches() {
        let schema = ColumnSchema::new([
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let batches: Vec<_> = (0..4)
            .map(|_| {
                Batch::from_arrays(vec![
                    Array::try_from_iter([Some("a")]).unwrap(),
                    Array::try_from_iter([Some(1)]).unwrap(),
                ])
                .unwrap()
            })
            .collect();

        let table = pretty_format_batches(&schema, &batches, 80, None).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │ b     │",
            "│ Utf8 │ Int32 │",
            "├──────┼───────┤",
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
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let create_batch = |s, n| {
            Batch::from_arrays([
                Array::try_from_iter([Some(s)]).unwrap(),
                Array::try_from_iter([Some(n)]).unwrap(),
            ])
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

        let table = pretty_format_batches(&schema, &batches, 80, Some(4)).unwrap();

        let expected = [
            "┌────────┬────────┐",
            "│ a      │ b      │",
            "│ Utf8   │ Int32  │",
            "├────────┼────────┤",
            "│ a      │      1 │",
            "│ b      │      2 │",
            "│ …      │      … │",
            "│ e      │      5 │",
            "│ f      │      6 │",
            "├────────┴────────┤",
            "│ 6 rows, 4 shown │",
            "└─────────────────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn large_batch_with_max_rows() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let a_vals = (0..10).map(|v| v.to_string());
        let b_vals = (0..10).map(Some);

        let batches = vec![
            Batch::from_arrays(vec![
                Array::try_from_iter(a_vals).unwrap(),
                Array::try_from_iter(b_vals).unwrap(),
            ])
            .unwrap(),
        ];

        let table = pretty_format_batches(&schema, &batches, 80, Some(4)).unwrap();

        let expected = [
            "┌────────┬─────────┐",
            "│ a      │ b       │",
            "│ Utf8   │ Int32   │",
            "├────────┼─────────┤",
            "│ 0      │       0 │",
            "│ 1      │       1 │",
            "│ …      │       … │",
            "│ 8      │       8 │",
            "│ 9      │       9 │",
            "├────────┴─────────┤",
            "│ 10 rows, 4 shown │",
            "└──────────────────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn large_batch_with_odd_max_rows() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let a_vals: Vec<_> = (0..10).map(|v| Some(v.to_string())).collect();
        let b_vals: Vec<_> = (0..10).map(Some).collect();

        let batches = vec![
            Batch::from_arrays(vec![
                Array::try_from_iter(a_vals).unwrap(),
                Array::try_from_iter(b_vals).unwrap(),
            ])
            .unwrap(),
        ];

        let table = pretty_format_batches(&schema, &batches, 80, Some(3)).unwrap();

        let expected = [
            "┌────────┬─────────┐",
            "│ a      │ b       │",
            "│ Utf8   │ Int32   │",
            "├────────┼─────────┤",
            "│ 0      │       0 │",
            "│ 1      │       1 │",
            "│ …      │       … │",
            "│ 9      │       9 │",
            "├────────┴─────────┤",
            "│ 10 rows, 3 shown │",
            "└──────────────────┘",
        ]
        .join("\n");

        assert_eq_print(expected, table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_value() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
            Field::new("c", DataType::utf8(), true),
            Field::new("d", DataType::utf8(), true),
        ]);

        let create_batch = |a, b, c, d| {
            Batch::from_arrays(vec![
                Array::try_from_iter([Some(a)]).unwrap(),
                Array::try_from_iter([Some(b)]).unwrap(),
                Array::try_from_iter([Some(c)]).unwrap(),
                Array::try_from_iter([Some(d)]).unwrap(),
            ])
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 1, "c", "d"),
            create_batch("a", 2, "ccccc", "d"),
            create_batch("a", 3, "cccccccccc", "d"),
            create_batch("a", 4, "cccccccccccccccccc", "d"),
        ];

        let table = pretty_format_batches(&schema, &batches, 40, None).unwrap();

        let expected = [
            "┌──────┬───────┬────────────────┬──────┐",
            "│ a    │ b     │ c              │ d    │",
            "│ Utf8 │ Int32 │ Utf8           │ Utf8 │",
            "├──────┼───────┼────────────────┼──────┤",
            "│ a    │     1 │ c              │ d    │",
            "│ a    │     2 │ ccccc          │ d    │",
            "│ a    │     3 │ cccccccccc     │ d    │",
            "│ a    │     4 │ ccccccccccccc… │ d    │",
            "└──────┴───────┴────────────────┴──────┘",
        ];

        // I'm just copy pasting output I'm getting. This is here to make sure
        // what's expected is actually correct.
        assert!(display_width(expected[0]) <= 40);

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_value_first() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
            Field::new("c", DataType::utf8(), true),
            Field::new("d", DataType::utf8(), true),
        ]);

        let create_batch = |a, b, c, d| {
            Batch::from_arrays(vec![
                Array::try_from_iter([Some(a)]).unwrap(),
                Array::try_from_iter([Some(b)]).unwrap(),
                Array::try_from_iter([Some(c)]).unwrap(),
                Array::try_from_iter([Some(d)]).unwrap(),
            ])
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 4, "cccccccccccccccccc", "d"),
            create_batch("a", 3, "cccccccccc", "d"),
            create_batch("a", 2, "ccccc", "d"),
            create_batch("a", 1, "c", "d"),
        ];

        let table = pretty_format_batches(&schema, &batches, 40, None).unwrap();

        let expected = [
            "┌──────┬───────┬────────────────┬──────┐",
            "│ a    │ b     │ c              │ d    │",
            "│ Utf8 │ Int32 │ Utf8           │ Utf8 │",
            "├──────┼───────┼────────────────┼──────┤",
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
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("thisisasomewhatlongcolumn", DataType::int32(), true),
            Field::new("c", DataType::utf8(), true),
        ]);

        let create_batch = |a, b, c| {
            Batch::from_arrays(vec![
                Array::try_from_iter([Some(a)]).unwrap(),
                Array::try_from_iter([Some(b)]).unwrap(),
                Array::try_from_iter([Some(c)]).unwrap(),
            ])
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 4, "cccccccccccccccccc"),
            create_batch("a", 3, "cccccccccc"),
            create_batch("a", 2, "ccccc"),
            create_batch("a", 1, "c"),
        ];

        let table = pretty_format_batches(&schema, &batches, 40, None).unwrap();

        let expected = [
            "┌────────────┬────────────┬────────────┐",
            "│ a          │ thisisaso… │ c          │",
            "│ Utf8       │ Int32      │ Utf8       │",
            "├────────────┼────────────┼────────────┤",
            "│ a          │          4 │ ccccccccc… │",
            "│ a          │          3 │ cccccccccc │",
            "│ a          │          2 │ ccccc      │",
            "│ a          │          1 │ c          │",
            "└────────────┴────────────┴────────────┘",
        ];
        assert!(display_width(expected[0]) <= 40);

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn multiple_small_batches_with_max_width_and_long_column_name_even_num_cols() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("thisisasomewhatlongcolumn", DataType::int32(), true),
            Field::new("c", DataType::utf8(), true),
            Field::new("d", DataType::utf8(), true),
        ]);

        let create_batch = |a, b, c, d| {
            Batch::from_arrays(vec![
                Array::try_from_iter([Some(a)]).unwrap(),
                Array::try_from_iter([Some(b)]).unwrap(),
                Array::try_from_iter([Some(c)]).unwrap(),
                Array::try_from_iter([Some(d)]).unwrap(),
            ])
            .unwrap()
        };

        let batches = vec![
            create_batch("a", 4, "cccccccccccccccccc", "d"),
            create_batch("a", 3, "cccccccccc", "d"),
            create_batch("a", 2, "ccccc", "d"),
            create_batch("a", 1, "c", "d"),
        ];

        let table = pretty_format_batches(&schema, &batches, 40, None).unwrap();

        let expected = [
            "┌──────────┬──────────┬───┬────────────┐",
            "│ a        │ thisisa… │ … │ d          │",
            "│ Utf8     │ Int32    │   │ Utf8       │",
            "├──────────┼──────────┼───┼────────────┤",
            "│ a        │        4 │ … │ d          │",
            "│ a        │        3 │ … │ d          │",
            "│ a        │        2 │ … │ d          │",
            "│ a        │        1 │ … │ d          │",
            "└──────────┴──────────┴───┴────────────┘",
        ];

        assert!(display_width(expected[0]) <= 40);

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn many_cols_small_max_width() {
        // https://github.com/GlareDB/glaredb/issues/1790

        let fields: Vec<_> = (0..30)
            .map(|i| Field::new(i.to_string(), DataType::int8(), true))
            .collect();

        let schema = ColumnSchema::new(fields);

        let table = pretty_format_batches(&schema, &[], 40, None).unwrap();

        let expected = [
            "┌──────┬──────┬───┬──────┬──────┐",
            "│ 0    │ 1    │ … │ 28   │ 29   │",
            "│ Int8 │ Int8 │   │ Int8 │ Int8 │",
            "├──────┴──────┴───┴──────┴──────┤",
            "│ No rows                       │",
            "└───────────────────────────────┘",
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

        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        // First record should be printed.
        let first = Batch::from_arrays(vec![
            Array::try_from_iter([Some("1"), Some("2")]).unwrap(),
            Array::try_from_iter([Some(1), Some(2)]).unwrap(),
        ])
        .unwrap();

        // Nothing in this batch should be printed.
        let middle = Batch::from_arrays(vec![
            Array::try_from_iter([Some("3"), Some("4")]).unwrap(),
            Array::try_from_iter([Some(3), Some(4)]).unwrap(),
        ])
        .unwrap();

        // Last record should be printed.
        let last = Batch::from_arrays(vec![
            Array::try_from_iter([Some("5"), Some("6")]).unwrap(),
            Array::try_from_iter([Some(5), Some(6)]).unwrap(),
        ])
        .unwrap();

        let table = pretty_format_batches(&schema, &[first, middle, last], 40, Some(2)).unwrap();

        let expected = [
            "┌────────┬────────┐",
            "│ a      │ b      │",
            "│ Utf8   │ Int32  │",
            "├────────┼────────┤",
            "│ 1      │      1 │",
            "│ …      │      … │",
            "│ 6      │      6 │",
            "├────────┴────────┤",
            "│ 6 rows, 2 shown │",
            "└─────────────────┘",
        ];

        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn batch_with_selected_rows() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let mut batch = Batch::from_arrays(vec![
            Array::try_from_iter([Some("1"), Some("2")]).unwrap(),
            Array::try_from_iter([Some(1), Some(2)]).unwrap(),
        ])
        .unwrap();

        batch.select(Selection::slice(&[0, 0, 1, 1, 0, 0])).unwrap();

        let table = pretty_format_batches(&schema, &[batch], 40, None).unwrap();

        let expected = [
            "┌──────┬───────┐",
            "│ a    │ b     │",
            "│ Utf8 │ Int32 │",
            "├──────┼───────┤",
            "│ 1    │     1 │",
            "│ 1    │     1 │",
            "│ 2    │     2 │",
            "│ 2    │     2 │",
            "│ 1    │     1 │",
            "│ 1    │     1 │",
            "└──────┴───────┘",
        ];
        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn batch_with_selected_rows_exceeds_max_rows() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let mut batch = Batch::from_arrays(vec![
            Array::try_from_iter([Some("1"), Some("2")]).unwrap(),
            Array::try_from_iter([Some(1), Some(2)]).unwrap(),
        ])
        .unwrap();

        batch.select(Selection::slice(&[0, 0, 1, 1, 0, 0])).unwrap();

        let table = pretty_format_batches(&schema, &[batch], 40, Some(5)).unwrap();

        let expected = [
            "┌────────┬────────┐",
            "│ a      │ b      │",
            "│ Utf8   │ Int32  │",
            "├────────┼────────┤",
            "│ 1      │      1 │",
            "│ 1      │      1 │",
            "│ 2      │      2 │",
            "│ …      │      … │",
            "│ 1      │      1 │",
            "│ 1      │      1 │",
            "├────────┴────────┤",
            "│ 6 rows, 5 shown │",
            "└─────────────────┘",
        ];
        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn batch_with_constant_rows_exceeds_max_rows() {
        let schema = ColumnSchema::new(vec![
            Field::new("a", DataType::utf8(), true),
            Field::new("b", DataType::int32(), true),
        ]);

        let mut batch = Batch::from_arrays(vec![
            Array::new_constant(&DefaultBufferManager, &"1".into(), 6).unwrap(),
            Array::new_constant(&DefaultBufferManager, &2.into(), 6).unwrap(),
        ])
        .unwrap();

        batch.select(Selection::slice(&[0, 0, 1, 1, 0, 0])).unwrap();

        let table = pretty_format_batches(&schema, &[batch], 40, Some(5)).unwrap();

        let expected = [
            "┌────────┬────────┐",
            "│ a      │ b      │",
            "│ Utf8   │ Int32  │",
            "├────────┼────────┤",
            "│ 1      │      2 │",
            "│ 1      │      2 │",
            "│ 1      │      2 │",
            "│ …      │      … │",
            "│ 1      │      2 │",
            "│ 1      │      2 │",
            "├────────┴────────┤",
            "│ 6 rows, 5 shown │",
            "└─────────────────┘",
        ];
        assert_eq_print(expected.join("\n"), table.to_string())
    }

    #[test]
    fn batch_with_whitespace_trailing_newline() {
        // This showcases the current (if suboptimal) behavior of not properly
        // taking into account newlines during column width sampling.
        //
        // See <https://github.com/GlareDB/glaredb/issues/3467>

        let schema = ColumnSchema::new([Field::new("a", DataType::utf8(), true)]);

        let batch = generate_batch!(["yoshi mario\n"]);
        let table = pretty_format_batches(&schema, &[batch], 40, Some(5)).unwrap();

        let expected = [
            "┌───────────────┐",
            "│ a             │",
            "│ Utf8          │",
            "├───────────────┤",
            "│ yoshi mario + │",
            "│               │",
            "└───────────────┘",
        ];
        assert_eq_print(expected.join("\n"), table.to_string())
    }
}
