use std::fmt::{self, Write as _};

use super::components::TableComponents;
use super::table::ColumnValues;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrettyHeader {
    components: &'static TableComponents,
    column_widths: Vec<usize>,
    headers: Vec<ColumnValues>,
    table_has_values: bool,
}

impl PrettyHeader {
    pub fn new(
        components: &'static TableComponents,
        column_widths: Vec<usize>,
        headers: Vec<ColumnValues>,
        table_has_values: bool,
    ) -> Self {
        debug_assert_eq!(headers.len(), column_widths.len());
        PrettyHeader {
            components,
            column_widths,
            headers,
            table_has_values,
        }
    }
}

impl fmt::Display for PrettyHeader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Write the top border.
        f.write_char(self.components.ulc)?;
        for (idx, &width) in self.column_widths.iter().enumerate() {
            f.write_char(self.components.tb)?;
            for _ in 0..width {
                f.write_char(self.components.tb)?;
            }
            f.write_char(self.components.tb)?;

            if idx < self.headers.len() - 1 {
                f.write_char(self.components.tb_int)?;
            }
        }
        f.write_char(self.components.urc)?;
        f.write_char('\n')?;

        // Write first header line.
        f.write_char(self.components.vert)?;
        for (header, &width) in self.headers.iter().zip(self.column_widths.iter()) {
            f.write_char(' ')?;
            write!(f, "{:width$}", header.value(0))?;
            f.write_char(' ')?;

            f.write_char(self.components.vert)?;
        }
        f.write_char('\n')?;

        // Write second header line.
        f.write_char(self.components.vert)?;
        for (header, &width) in self.headers.iter().zip(self.column_widths.iter()) {
            f.write_char(' ')?;
            write!(f, "{:width$}", header.value(1))?;
            f.write_char(' ')?;

            f.write_char(self.components.vert)?;
        }
        f.write_char('\n')?;

        // Write header separator.
        if self.table_has_values {
            f.write_char(self.components.lb_int)?;
            for (idx, &width) in self.column_widths.iter().enumerate() {
                f.write_char(self.components.hor)?;
                for _ in 0..width {
                    f.write_char(self.components.hor)?;
                }
                f.write_char(self.components.hor)?;

                if idx < self.headers.len() - 1 {
                    f.write_char(self.components.intersection)?;
                }
            }
            f.write_char(self.components.rb_int)?;
            f.write_char('\n')?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Alignment {
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrettyValues {
    components: &'static TableComponents,
    alignments: Vec<Alignment>,
    column_widths: Vec<usize>,
    columns: Vec<ColumnValues>,
    row_heights: Vec<usize>,
}

impl PrettyValues {
    pub fn new(
        components: &'static TableComponents,
        alignments: Vec<Alignment>,
        column_widths: Vec<usize>,
        columns: Vec<ColumnValues>,
    ) -> Self {
        debug_assert_eq!(alignments.len(), column_widths.len());
        debug_assert_eq!(columns.len(), column_widths.len());

        let num_rows = match columns.first() {
            Some(col) => col.num_values(),
            None => 0,
        };

        let mut row_heights = vec![1; num_rows];

        for col in &columns {
            for (&row, &height) in col.row_heights.iter() {
                if row_heights[row] < height {
                    row_heights[row] = height;
                }
            }
        }

        PrettyValues {
            components,
            alignments,
            column_widths,
            columns,
            row_heights,
        }
    }
}

impl fmt::Display for PrettyValues {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let num_rows = match self.columns.first() {
            Some(c) => c.num_values(),
            None => return Ok(()),
        };

        for row in 0..num_rows {
            if self.row_heights[row] == 1 {
                // Simple case, row is only a single line.
                f.write_char(self.components.vert)?;
                for (column, (&width, alignment)) in self
                    .columns
                    .iter()
                    .zip(self.column_widths.iter().zip(self.alignments.iter()))
                {
                    f.write_char(' ')?;
                    match alignment {
                        Alignment::Left => write!(f, "{:<width$}", column.value(row))?,
                        Alignment::Right => write!(f, "{:>width$}", column.value(row))?,
                    }
                    f.write_char(' ')?;

                    f.write_char(self.components.vert)?;
                }
                f.write_char('\n')?;
            } else {
                // Complex case, one or more values in the row takes up more
                // than one line.
                let mut col_lines: Vec<_> =
                    self.columns.iter().map(|c| c.value(row).lines()).collect();

                for _n in 0..self.row_heights[row] {
                    f.write_char(self.components.vert)?;
                    for (col_idx, width) in self.column_widths.iter().enumerate() {
                        f.write_char(' ')?;
                        let val = col_lines[col_idx].next().unwrap_or("");
                        match self.alignments[col_idx] {
                            Alignment::Left => write!(f, "{val:<width$}")?,
                            Alignment::Right => write!(f, "{val:>width$}")?,
                        }
                        f.write_char(' ')?;

                        f.write_char(self.components.vert)?;
                    }

                    f.write_char('\n')?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrettyFooter {
    pub components: &'static TableComponents,
    pub content: String,
    pub column_widths: Vec<usize>,
}

impl fmt::Display for PrettyFooter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Characters depending on if we actually have a footer or not.
        let (left_border, intersection, right_border) = if self.content.is_empty() {
            (
                self.components.blc,
                self.components.bb_int,
                self.components.brc,
            )
        } else {
            (
                self.components.lb_int,
                self.components.bb_int,
                self.components.rb_int,
            )
        };

        f.write_char(left_border)?;
        for (idx, &width) in self.column_widths.iter().enumerate() {
            f.write_char(self.components.hor)?;
            for _ in 0..width {
                f.write_char(self.components.hor)?;
            }
            f.write_char(self.components.hor)?;

            if idx < self.column_widths.len() - 1 {
                f.write_char(intersection)?;
            }
        }
        f.write_char(right_border)?;

        if !self.content.is_empty() {
            f.write_char('\n')?;
            let table_width = table_width(&self.column_widths);

            f.write_char(self.components.lb)?;
            f.write_char(' ')?;

            let usable = table_width - 4; // Remove border and space padding.
            write!(f, "{:usable$}", self.content)?;

            f.write_char(' ')?;
            f.write_char(self.components.rb)?;
            f.write_char('\n')?;

            f.write_char(self.components.blc)?;
            let border_width = table_width - 2; // Remove corners
            for _ in 0..border_width {
                f.write_char(self.components.bb)?;
            }
            f.write_char(self.components.brc)?;
        }

        Ok(())
    }
}

pub fn table_width(column_widths: &[usize]) -> usize {
    let mut width = column_widths.iter().sum();

    // Each column has 1 space on the left, 1 space on the right, and 1 border
    // on the right.
    width += column_widths.len() * 3;

    // Include left-most border as well.
    width += 1;

    width
}
