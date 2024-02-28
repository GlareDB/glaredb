use crate::hash::build_hashes;
use crate::planner::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::planner::operator::JoinType;
use crate::types::batch::{DataBatch, DataBatchSchema};
use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{Field, Schema};
use hashbrown::raw::RawTable;
use parking_lot::Mutex;
use rayexec_error::{RayexecError, Result};
use smallvec::{smallvec, SmallVec};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use super::{buffer::BatchBuffer, Sink, Source};

#[derive(Debug)]
pub struct PhysicalHashJoin {
    schema: DataBatchSchema,
    join_type: JoinType,
    // (left, right) indices pairs.
    join_on: Vec<(usize, usize)>,
    buffer: BatchBuffer,
}

impl Source for PhysicalHashJoin {
    fn output_partitions(&self) -> usize {
        self.buffer.output_partitions()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<DataBatch>>> {
        self.buffer.poll_partition(cx, partition)
    }
}

impl Sink for PhysicalHashJoin {
    fn push(&self, input: DataBatch, child: usize, partition: usize) -> Result<()> {
        unimplemented!()
    }

    fn finish(&self, child: usize, partition: usize) -> Result<()> {
        unimplemented!()
    }
}

impl Explainable for PhysicalHashJoin {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("HashJoin")
    }
}

struct BuildStage {
    tables: Vec<Mutex<PartitionHashTable>>,
    num_building: AtomicUsize,
}

impl BuildStage {
    fn new(num_partitions: usize) -> Self {
        BuildStage {
            tables: (0..num_partitions)
                .map(|_i| Mutex::new(PartitionHashTable::default()))
                .collect(),
            num_building: num_partitions.into(),
        }
    }

    fn insert_for_partition(
        &self,
        partition: usize,
        batch: RecordBatch,
        hash_cols: &[usize],
    ) -> Result<()> {
        let mut table = self.tables[partition].lock();
        table.insert_batch(batch, hash_cols)?;
        Ok(())
    }
}

#[derive(Default)]
struct PartitionHashTable {
    batches: Vec<HashedBatch>,
    hashes_buf: Vec<u64>,
}

impl PartitionHashTable {
    /// Inserts a batch into the hash table, using the specified column indices
    /// for building the hash.
    fn insert_batch(&mut self, batch: RecordBatch, key_cols: &[usize]) -> Result<()> {
        let batch = HashedBatch::from_record_batch(batch, key_cols, &mut self.hashes_buf)?;
        self.batches.push(batch);
        Ok(())
    }
}

struct ProbeStage {
    batches: Vec<HashedBatch>,
}

impl ProbeStage {
    fn probe(
        &self,
        probe: &RecordBatch,
        key_cols: &[usize],
        join_type: JoinType,
        output_schema: Arc<Schema>,
    ) -> Result<Vec<RecordBatch>> {
        let probe_arrs: Vec<_> = key_cols
            .iter()
            .map(|idx| probe.columns().get(*idx).expect("valid column indexes"))
            .collect();

        let mut hashes = vec![0; probe.num_rows()]; // TODO: Don't allocate every time.
        build_hashes(&probe_arrs, &mut hashes)?;

        let mut output_batches: Vec<RecordBatch> = Vec::with_capacity(self.batches.len());

        for batch in self.batches.iter() {
            // Check equality based on hashes.
            let mut partition_build_indexes: Vec<u32> = Vec::new();
            let mut partition_probe_indexes: Vec<u32> = Vec::new();

            for (probe_row_idx, probe_row_hash) in hashes.iter().enumerate() {
                let entry = batch
                    .table
                    .get(*probe_row_hash, |(hash, _)| probe_row_hash == hash);
                if let Some((_, indexes)) = entry {
                    for build_index in indexes {
                        partition_build_indexes.push(*build_index as u32);
                        partition_probe_indexes.push(probe_row_idx as u32);
                    }
                }
            }

            let mut partition_build_indexes = UInt32Array::from(partition_build_indexes);
            let mut partition_probe_indexes = UInt32Array::from(partition_probe_indexes);

            // Check equality based on key values.
            let build_arrs: Vec<_> = key_cols
                .iter()
                .map(|idx| {
                    batch
                        .batch
                        .columns()
                        .get(*idx)
                        .expect("valid column indexes")
                })
                .collect();

            let mut equal = BooleanArray::from(vec![true; partition_probe_indexes.len()]);
            let iter = build_arrs.iter().zip(probe_arrs.iter());
            equal = iter
                .map(|(build, probe)| {
                    let build_take = arrow::compute::take(&build, &partition_build_indexes, None)?;
                    let probe_take = arrow::compute::take(&probe, &partition_probe_indexes, None)?;
                    // TODO: null == null
                    arrow_ord::cmp::eq(&build_take, &probe_take)
                })
                .try_fold(equal, |acc, result| arrow::compute::and(&acc, &result?))?;

            partition_build_indexes = arrow_array::cast::downcast_array(&arrow::compute::filter(
                &partition_build_indexes,
                &equal,
            )?);
            partition_probe_indexes = arrow_array::cast::downcast_array(&arrow::compute::filter(
                &partition_probe_indexes,
                &equal,
            )?);

            // Adjust indexes as needed depending on the join type.
            // TODO: Handle everything else.
            match join_type {
                JoinType::Inner => {
                    // No adjustments needed.
                }
                _ => unimplemented!(),
            }

            // Build final record batch.
            let mut cols = Vec::with_capacity(output_schema.fields.len());
            for build_col in batch.batch.columns() {
                cols.push(arrow::compute::take(
                    build_col,
                    &partition_build_indexes,
                    None,
                )?);
            }
            for probe_col in probe.columns() {
                cols.push(arrow::compute::take(
                    probe_col,
                    &partition_probe_indexes,
                    None,
                )?)
            }

            let output = RecordBatch::try_new(output_schema.clone(), cols)?;
            output_batches.push(output);
        }

        Ok(output_batches)
    }
}

/// A record batch along with key hashes.
struct HashedBatch {
    table: RawTable<(u64, SmallVec<[usize; 2]>)>,
    batch: RecordBatch,
}

impl HashedBatch {
    /// Create a hashes batch from a record batch, using the provided indexes to
    /// build the hashes.
    fn from_record_batch(
        batch: RecordBatch,
        key_cols: &[usize],
        hash_buf: &mut Vec<u64>,
    ) -> Result<Self> {
        let arrs: Vec<_> = key_cols
            .iter()
            .map(|idx| batch.columns().get(*idx).expect("valid column indexes"))
            .collect();

        hash_buf.resize(batch.num_rows(), 0);
        build_hashes(&arrs, hash_buf)?;

        let mut table: RawTable<(u64, SmallVec<_>)> = RawTable::with_capacity(hash_buf.len());

        for (row_index, row_hash) in hash_buf.iter().enumerate() {
            if let Some((_, indexes)) = table.get_mut(*row_hash, |(hash, _)| row_hash == hash) {
                indexes.push(row_index);
            } else {
                table.insert(*row_hash, (*row_hash, smallvec![row_index]), |(hash, _)| {
                    *hash
                });
            }
        }

        Ok(HashedBatch { table, batch })
    }
}
