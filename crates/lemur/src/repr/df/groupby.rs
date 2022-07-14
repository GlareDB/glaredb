use super::*;
use crate::repr::compute::VecUnaryAggregate;
use crate::repr::sort::{GroupRanges, SortPermutation};
use crate::repr::value::ValueVec;
use anyhow::{anyhow, Result};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;

pub type AccumulatorFn = Box<dyn Fn(&ValueVec, &GroupRanges) -> Result<ValueVec> + Send>;

/// An accumulating function assigned to a column.
pub struct Accumulator {
    func: AccumulatorFn,
    column: usize,
}

impl Accumulator {
    pub fn with_func<F>(column: usize, func: F) -> Self
    where
        F: Fn(&ValueVec, &GroupRanges) -> Result<ValueVec> + Send + 'static,
    {
        Accumulator {
            func: Box::new(func),
            column,
        }
    }

    /// Create an accumulator that persists the first value found.
    pub fn first_value(column: usize) -> Self {
        Self::with_func(column, VecUnaryAggregate::first_groups)
    }
}

impl fmt::Debug for Accumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Figure out how to print out relevant info for the provided
        // function. Extend the AccumulatorFn type to include a debug string?
        write!(f, "accumulator, column: {}", &self.column)
    }
}

// TODO: Use this.
#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// A dataframe supporting accumulating results over some number of groups.
///
/// This will also implicitly sort the vectors based on the grouping indexes.
#[derive(Debug, Clone)]
pub struct SortedGroupByDataFrame {
    columns: Vec<Arc<ValueVec>>,
    groups: GroupRanges,
}

impl SortedGroupByDataFrame {
    /// Create a grouped dataframe using the provided column indexes for
    /// grouping.
    ///
    /// Groups will be sorted according to the indexes provided. E.g. if the the
    /// provided column indexes are `1` and `0`, the entire dataframe will be
    /// sorted by column `1`, then each group found in column `1` will be sorted
    /// by column `0`.
    ///
    /// If no indexes are provided, this dataframe will act as if the entire
    /// data set is in one group. No sorting will be done in such cases.
    pub fn from_dataframe(df: DataFrame, grouping_idxs: &[usize]) -> Result<Self> {
        let mut curr_groups = GroupRanges::single_group(df.num_rows());
        let mut columns = df.columns;

        for &grouping_idx in grouping_idxs.iter() {
            let col = columns
                .get_mut(grouping_idx)
                .ok_or(anyhow!("missing column for grouping"))?;

            // Sort the column according to any previously defined groups.
            // TODO: Check if sorted before making mut.
            let perms = (*Arc::make_mut(col)).sort_each_group(&curr_groups);
            std::mem::drop(col);

            // Apply the permutations resulting from the sort to the current
            // groups.
            for col in columns.iter_mut() {
                for (group, perm) in curr_groups.ranges.iter().zip(perms.iter()) {
                    (*Arc::make_mut(col)).apply_permutation_at(group, perm);
                }
            }

            // Get the new set of groups from the now sorted column.
            let col = columns.get(grouping_idx).unwrap();
            curr_groups = curr_groups
                .ranges
                .iter()
                .map(|range| col.group_ranges_at(range))
                .fold(GroupRanges::no_group(), |mut acc, group| {
                    acc.make_contiguous(group);
                    acc
                });
        }

        Ok(SortedGroupByDataFrame {
            columns,
            groups: curr_groups,
        })
    }

    /// Create a dataframe out of self.
    ///
    /// Useful for if we're only sorting without accumulating.
    pub fn into_dataframe(self) -> DataFrame {
        DataFrame {
            columns: self.columns,
        }
    }

    /// Accumulate over all of the groups using the given accumulators.
    ///
    /// Every accumulating function must produce a vector whose size matches the
    /// number of groups being accumulated over. Columns may be referenced more
    /// than once.
    ///
    /// The resulting dataframe will contain only the results of accumulation.
    pub fn accumulate(self, fns: Vec<Accumulator>) -> Result<DataFrame> {
        let mut columns = Vec::with_capacity(fns.len());

        for acc in fns {
            let col = self
                .columns
                .get(acc.column)
                .ok_or(anyhow!("missing column for accumulating: {}", acc.column))?;
            let out = (acc.func)(col, &self.groups)?;
            columns.push(out);
        }

        DataFrame::from_columns(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repr::compute::{VecCountAggregate, VecNumericAggregate};

    #[test]
    fn no_group() {
        let df = DataFrame::from_columns([
            ValueVec::bools(&[true, false, false, true, true]),
            ValueVec::int32s(&[1, 8, 7, 2, 9]),
        ])
        .unwrap();

        let grouped = SortedGroupByDataFrame::from_dataframe(df.clone(), &[]).unwrap();
        let out = grouped.into_dataframe();

        assert_eq!(df, out);
    }

    #[test]
    fn simple_order_by() {
        let df = DataFrame::from_columns([
            ValueVec::bools(&[true, false, false, true, true]),
            ValueVec::int8s(&[6, 5, 4, 5, 6]),
            ValueVec::int32s(&[1, 8, 7, 2, 9]),
        ])
        .unwrap();

        let grouped = SortedGroupByDataFrame::from_dataframe(df, &[1, 2]).unwrap();

        let expected = DataFrame::from_columns([
            ValueVec::bools(&[false, true, false, true, true]),
            ValueVec::int8s(&[4, 5, 5, 6, 6]),
            ValueVec::int32s(&[7, 2, 8, 1, 9]),
        ])
        .unwrap();

        let got = grouped.into_dataframe();
        assert_eq!(expected, got);
    }

    #[test]
    fn simple_accumulate() {
        let df = DataFrame::from_columns([
            ValueVec::int32s(&[1, 3, 2, 3]),
            ValueVec::int32s(&[5, 7, 6, 8]),
        ])
        .unwrap();

        let grouped = SortedGroupByDataFrame::from_dataframe(df, &[0]).unwrap();
        let accumulated = grouped
            .accumulate(vec![
                Accumulator::first_value(0),
                Accumulator::with_func(1, VecNumericAggregate::sum_groups),
                Accumulator::with_func(1, VecCountAggregate::count_groups),
            ])
            .unwrap();

        let expected = DataFrame::from_columns([
            ValueVec::int32s(&[1, 2, 3]),  // Groups.
            ValueVec::int32s(&[5, 6, 15]), // Sums for each group.
            ValueVec::int32s(&[1, 1, 2]),  // Counts for each group.
        ])
        .unwrap();

        assert_eq!(expected, accumulated);
    }

    #[test]
    fn accumulate_no_grouping() {
        let df = DataFrame::from_columns([
            ValueVec::int32s(&[1, 3, 2, 3]),
            ValueVec::int32s(&[5, 7, 6, 8]),
        ])
        .unwrap();

        let grouped = SortedGroupByDataFrame::from_dataframe(df, &[]).unwrap();
        let accumulated = grouped
            .accumulate(vec![
                Accumulator::with_func(0, VecNumericAggregate::sum_groups),
                Accumulator::with_func(1, VecNumericAggregate::sum_groups),
                Accumulator::with_func(1, VecCountAggregate::count_groups),
            ])
            .unwrap();

        let expected = DataFrame::from_columns([
            ValueVec::int32s(&[9]),  // Sum of first column.
            ValueVec::int32s(&[26]), // Sum of second column.
            ValueVec::int32s(&[4]),  // Count of second column.
        ])
        .unwrap();

        assert_eq!(expected, accumulated);
    }
}
