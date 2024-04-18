use arrow::row::{RowConverter, Rows};
use arrow_array::{cast::AsArray, ArrayRef, UInt64Array};
use hashbrown::{raw::RawTable, HashSet};
use rayexec_error::Result;
use smallvec::{smallvec, SmallVec};
use std::fmt;

use crate::{functions::aggregate::Accumulator, types::batch::DataBatch};

#[derive(Debug)]
pub struct GroupingSetColumns<'a> {
    /// Columns making up the grouping set keys.
    ///
    /// For example, a `GROUP BY b, c` would have these contain columns b and c.
    pub columns: &'a [ArrayRef],

    /// The hashes for the above columns.
    ///
    /// Guaranteed to be the same length at the number of rows in the above
    /// columns.
    pub hashes: &'a [u64],
}

/// Hash table for a single grouping set.
pub struct GroupingSetHashTable {
    /// Hash table mapping from hash value to the index in `values` for the
    /// corresponding value.
    table: RawTable<(u64, usize)>,

    /// Converter for converting arrow arrays to rows.
    converter: RowConverter,

    /// All values found for the grouping set.
    values: Rows,
}

impl GroupingSetHashTable {
    pub fn insert_groups<'a>(
        &mut self,
        groups: GroupingSetColumns,
        group_indexes: &'a mut Vec<usize>,
    ) -> Result<()> {
        let group_rows = self.converter.convert_columns(groups.columns)?;
        assert_eq!(group_rows.num_rows(), groups.hashes.len());

        group_indexes.clear();

        for (row_idx, hash) in groups.hashes.iter().enumerate() {
            let ent = self.table.get_mut(*hash, |(_hash, group_idx)| {
                group_rows.row(row_idx) == self.values.row(*group_idx)
            });

            // Get group index for this group.
            let group_idx = match ent {
                Some((_hash, group_idx)) => *group_idx, // Group already exists.
                None => {
                    // Group hasn't been seen before. Insert it into the table.
                    let group_idx = self.values.num_rows();
                    self.values.push(group_rows.row(row_idx));

                    self.table
                        .insert(*hash, (*hash, group_idx), |(hash, _)| *hash);

                    group_idx
                }
            };

            group_indexes.push(group_idx)
        }

        Ok(())
    }

    pub fn merge_from(&mut self, other: &mut Self) -> Result<()> {
        unimplemented!()
    }
}

impl fmt::Debug for GroupingSetHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GroupingSetHashTable")
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct GroupingSetAccumulators {
    /// All accumulators for the query.
    accumulators: Vec<Box<dyn Accumulator>>,

    /// Columns inputs for each of the accumulators.
    inputs: Vec<Vec<usize>>,
}

impl GroupingSetAccumulators {
    /// Update accumulator states for this grouping set from the provide input.
    pub fn update_groups(&mut self, batch: &DataBatch, group_indexes: &[usize]) -> Result<()> {
        let unique: HashSet<_> = group_indexes.iter().collect();

        let indexes = UInt64Array::from_iter_values(group_indexes.iter().map(|i| *i as u64));

        for &group_idx in unique {
            let group = UInt64Array::from_value(group_idx as u64, indexes.len());
            let selection = arrow_ord::cmp::eq(&indexes, &group)?;

            let filtered = batch
                .columns()
                .iter()
                .map(|a| arrow::compute::filter(a, &selection))
                .collect::<Result<Vec<_>, _>>()?;

            for (accumulator, input_cols) in self.accumulators.iter_mut().zip(self.inputs.iter()) {
                let arrs: Vec<_> = input_cols
                    .iter()
                    .map(|idx| filtered.get(*idx).expect("column to exist"))
                    .collect();

                accumulator.accumulate(group_idx, &arrs)?;
            }
        }

        Ok(())
    }

    pub fn merge_from(&mut self, other: &mut Self) -> Result<()> {
        unimplemented!()
    }
}
