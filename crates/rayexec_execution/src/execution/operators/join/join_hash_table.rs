use hashbrown::raw::RawTable;
use rayexec_bullet::{
    array::{Array, BooleanArray},
    batch::Batch,
    bitmap::Bitmap,
    compute::{concat::concat, filter::filter, take::take},
};
use rayexec_error::{RayexecError, Result};
use std::{collections::HashMap, fmt};

/// Points to a row in the hash table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RowKey {
    /// Index of the batch in the batches vector.
    batch_idx: usize,

    /// Index of the row in the batch.
    row_idx: usize,
}

/// Hash table for storing batches for a single partition.
pub struct PartitionJoinHashTable {
    /// Collected batches so far.
    ///
    /// Could be extended to be spillable.
    batches: Vec<Batch>,

    /// Hash table pointing to a row.
    hash_table: RawTable<(u64, RowKey)>,
}

impl PartitionJoinHashTable {
    pub fn new() -> Self {
        PartitionJoinHashTable {
            batches: Vec::new(),
            hash_table: RawTable::new(),
        }
    }

    /// Insert a batch into the hash table.
    ///
    /// `hash_indices` indicates which columns in the batch was used to compute
    /// the hashes.
    ///
    /// `selection` is a bitmap for selecting only a subset of the batch to
    /// insert into this hashmap.
    pub fn insert_batch(&mut self, batch: &Batch, hashes: &[u64], selection: Bitmap) -> Result<()> {
        assert_eq!(batch.num_rows(), hashes.len());

        let selection = BooleanArray::new_with_values(selection); // TODO: I don't like needing to wrap the bitmap.
        let filtered_arrs = batch
            .columns()
            .iter()
            .map(|arr| filter(arr.as_ref(), &selection))
            .collect::<Result<Vec<_>>>()?;
        let batch = Batch::try_new(filtered_arrs)?;

        if batch.num_rows() == 0 {
            return Ok(());
        }

        let batch_idx = self.batches.len();
        self.batches.push(batch);

        for (row_idx, (hash, _)) in hashes
            .iter()
            .zip(selection.values().iter())
            .filter(|(_, sel)| *sel)
            .enumerate()
        {
            let row_key = RowKey { batch_idx, row_idx };
            self.hash_table
                .insert(*hash, (*hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }

    /// Merge some other hash table into this one.
    pub fn merge(&mut self, mut other: Self) -> Result<()> {
        let batch_offset = self.batches.len();

        // Append all batches from other. When we drain the hash table, we'll
        // update the row keys to account for the new offset.
        self.batches.append(&mut other.batches);

        for (hash, mut row_key) in other.hash_table.drain() {
            row_key.batch_idx += batch_offset;
            self.hash_table
                .insert(hash, (hash, row_key), |(hash, _)| *hash);
        }

        Ok(())
    }

    // inner
    pub fn probe(
        &self,
        right: &Batch,
        hashes: &[u64],
        right_col_indices: &[usize],
    ) -> Result<Batch> {
        // Track per-batch row indices that match the input columns.
        //
        // The value is a vec of (left_idx, right_idx) pairs pointing to rows in
        // the left (build) and right (probe) batches respectively
        let mut row_indices: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();

        // TODO: Use this in the below equality check.
        let _right_cols = right_col_indices
            .iter()
            .map(|idx| right.column(*idx).map(|arr| arr.as_ref()))
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| RayexecError::new("missing column in input"))?;

        for (right_idx, hash) in hashes.iter().enumerate() {
            let val = self.hash_table.get(*hash, |(_, _key)| {
                // TODO: Use key to check that the row this key is pointing
                // equals the row in the input columns.
                true
            });
            if let Some(val) = val {
                use std::collections::hash_map::Entry;

                let row_key = val.1;
                match row_indices.entry(row_key.batch_idx) {
                    Entry::Occupied(mut ent) => ent.get_mut().push((row_key.row_idx, right_idx)),
                    Entry::Vacant(ent) => {
                        ent.insert(vec![(row_key.row_idx, right_idx)]);
                    }
                }
            }
        }

        // Get all rows from the left and right batches.
        //
        // The final batch will be a batch containing all columns from the left
        // and all columns from the right.
        let mut batches = Vec::with_capacity(row_indices.len());
        for (batch_idx, row_indices) in row_indices {
            let (left_rows, right_rows): (Vec<_>, Vec<_>) = row_indices.into_iter().unzip();

            let left_batch = self.batches.get(batch_idx).expect("batch to exist");
            let left_cols = left_batch
                .columns()
                .iter()
                .map(|arr| take(arr.as_ref(), &left_rows))
                .collect::<Result<Vec<_>>>()?;

            let right_cols = right
                .columns()
                .iter()
                .map(|arr| take(arr.as_ref(), &right_rows))
                .collect::<Result<Vec<_>>>()?;

            let all_cols = left_cols.into_iter().chain(right_cols.into_iter());

            let batch = Batch::try_new(all_cols)?;
            batches.push(batch);
        }

        // Concat all batches.
        //
        // TODO: I _think_ it might be better to just return the computed
        // `row_indices` map and have another method that accepts that along
        // with a desired batch size to avoid creating a single very large batch
        // on one probe call.

        let num_cols = match batches.first() {
            Some(batch) => batch.columns().len(),
            None => return Ok(Batch::empty()),
        };

        let mut output_cols = Vec::with_capacity(num_cols);
        for col_idx in 0..num_cols {
            let cols: Vec<_> = batches
                .iter()
                .map(|batch| batch.column(col_idx).expect("column to exist").as_ref())
                .collect();

            output_cols.push(concat(&cols)?);
        }

        let batch = Batch::try_new(output_cols)?;

        Ok(batch)
    }
}

impl fmt::Debug for PartitionJoinHashTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionHashTable").finish_non_exhaustive()
    }
}
