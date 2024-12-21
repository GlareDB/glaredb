use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::stream::Stream;
use futures::{StreamExt, TryStreamExt};
use rayexec_bullet::array::Array;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::field::Schema;
use rayexec_bullet::format::pretty::table::PrettyTable;
use rayexec_bullet::row::ScalarRow;
use rayexec_error::{RayexecError, Result};
use rayexec_execution::engine::profiler::PlanningProfileData;
use rayexec_execution::engine::result::ExecutionResult;
use rayexec_execution::execution::executable::profiler::ExecutionProfileData;
use rayexec_execution::runtime::handle::QueryHandle;

#[derive(Debug)]
pub struct StreamingTable {
    pub(crate) result: ExecutionResult,
}

impl StreamingTable {
    pub fn schema(&self) -> &Schema {
        &self.result.output_schema
    }

    pub fn handle(&self) -> &Arc<dyn QueryHandle> {
        &self.result.handle
    }

    pub async fn collect(self) -> Result<MaterializedResultTable> {
        let batches: Vec<_> = self.result.stream.try_collect::<Vec<_>>().await?;

        Ok(MaterializedResultTable {
            schema: self.result.output_schema,
            batches,
            planning_profile: Some(self.result.planning_profile),
            execution_profile: None,
        })
    }

    pub async fn collect_with_execution_profile(self) -> Result<MaterializedResultTable> {
        let batches: Vec<_> = self.result.stream.try_collect::<Vec<_>>().await?;
        let execution_profile = self.result.handle.generate_execution_profile_data().await?;

        Ok(MaterializedResultTable {
            schema: self.result.output_schema,
            batches,
            planning_profile: Some(self.result.planning_profile),
            execution_profile: Some(execution_profile),
        })
    }

    pub async fn generate_profile_data(
        &self,
    ) -> Result<(PlanningProfileData, ExecutionProfileData)> {
        let execution_profile = self.result.handle.generate_execution_profile_data().await?;
        Ok((self.result.planning_profile.clone(), execution_profile))
    }
}

impl Stream for StreamingTable {
    type Item = Result<BatchOld>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.result.stream.poll_next_unpin(cx)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedResultTable {
    pub(crate) schema: Schema,
    pub(crate) batches: Vec<BatchOld>,
    pub(crate) planning_profile: Option<PlanningProfileData>,
    pub(crate) execution_profile: Option<ExecutionProfileData>,
}

impl MaterializedResultTable {
    /// Create a new materialized result table.
    ///
    /// Mostly for testing.
    pub fn try_new(schema: Schema, batches: impl IntoIterator<Item = BatchOld>) -> Result<Self> {
        let batches: Vec<_> = batches.into_iter().collect();
        for batch in &batches {
            if batch.columns().len() != schema.fields.len() {
                return Err(RayexecError::new(format!(
                    "Batch contains different number of columns than schema, batch: {}, schema: {}",
                    batch.columns().len(),
                    schema.fields.len()
                )));
            }

            for (col, field) in batch.columns().iter().zip(schema.fields.iter()) {
                let col_datatype = col.datatype();
                if col_datatype != &field.datatype {
                    return Err(RayexecError::new(format!(
                        "Unexpected column datatype, have: {}, want: {}",
                        col_datatype, field.datatype
                    )));
                }
            }
        }

        Ok(MaterializedResultTable {
            schema,
            batches,
            planning_profile: None,
            execution_profile: None,
        })
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn planning_profile_data(&self) -> Option<&PlanningProfileData> {
        self.planning_profile.as_ref()
    }

    pub fn execution_profile_data(&self) -> Option<&ExecutionProfileData> {
        self.execution_profile.as_ref()
    }

    pub fn pretty_table(&self, width: usize, max_rows: Option<usize>) -> Result<PrettyTable> {
        PrettyTable::try_new(&self.schema, &self.batches, width, max_rows)
    }

    pub fn iter_batches(&self) -> impl Iterator<Item = &BatchOld> {
        self.batches.iter()
    }

    pub fn iter_rows(&self) -> MaterializedRowIter {
        MaterializedRowIter {
            table: self,
            batch_idx: 0,
            row_idx: 0,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Execute a function for a cell in the table.
    ///
    /// The function will be passed the array containing the cell, and the row
    /// within that array.
    pub fn with_cell<F, T>(&self, cell_fn: F, col: usize, row: usize) -> Result<T>
    where
        F: Fn(&Array, usize) -> Result<T>,
    {
        let (batch_idx, row) = find_normalized_row(row, self.batches.iter().map(|b| b.num_rows()))
            .ok_or_else(|| RayexecError::new(format!("Row out of range: {}", row)))?;

        let batch = &self.batches[batch_idx];
        let arr = batch
            .column(col)
            .ok_or_else(|| RayexecError::new(format!("Column out of range: {}", col)))?;

        cell_fn(arr, row)
    }

    pub fn column_by_name(&self, name: &str) -> Result<MaterializedColumn> {
        let col_idx = self
            .schema
            .fields
            .iter()
            .position(|f| f.name == name)
            .ok_or_else(|| {
                RayexecError::new(format!(
                    "Unable to find column with name '{name}' in results table"
                ))
            })?;

        let arrays = self
            .batches
            .iter()
            .map(|b| b.column(col_idx).expect("column to exist").clone())
            .collect();

        Ok(MaterializedColumn { arrays })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedColumn {
    pub(crate) arrays: Vec<Array>,
}

impl MaterializedColumn {
    pub fn len(&self) -> usize {
        self.arrays.iter().map(|a| a.logical_len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn with_row<F, T>(&self, row_fn: F, row: usize) -> Result<T>
    where
        F: Fn(&Array, usize) -> Result<T>,
    {
        let (arr_idx, row) =
            find_normalized_row(row, self.arrays.iter().map(|arr| arr.logical_len()))
                .ok_or_else(|| RayexecError::new(format!("Row out of range: {}", row)))?;

        let arr = &self.arrays[arr_idx];
        row_fn(arr, row)
    }
}

#[inline]
fn find_normalized_row(
    mut row: usize,
    items_rows: impl IntoIterator<Item = usize>,
) -> Option<(usize, usize)> {
    for (idx, num_rows) in items_rows.into_iter().enumerate() {
        if row < num_rows {
            return Some((idx, row));
        }
        row -= num_rows;
    }
    None
}

#[derive(Debug, Clone, PartialEq)]
pub struct MaterializedRowIter<'a> {
    table: &'a MaterializedResultTable,
    batch_idx: usize,
    row_idx: usize,
}

impl<'a> Iterator for MaterializedRowIter<'a> {
    type Item = ScalarRow<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let batch = self.table.batches.get(self.batch_idx)?;

            // TODO: Reuse underlying vec
            match batch.row(self.row_idx) {
                Some(row) => {
                    self.row_idx += 1;
                    return Some(row);
                }
                None => {
                    // Try next batch.
                    self.row_idx = 0;
                    self.batch_idx += 1;
                }
            }
        }
    }
}
