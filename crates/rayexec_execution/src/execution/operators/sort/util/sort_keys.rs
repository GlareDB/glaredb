use rayexec_bullet::{
    array::Array,
    batch::Batch,
    row::encoding::{ComparableColumn, ComparableRowEncoder, ComparableRows},
};
use rayexec_error::{RayexecError, Result};

use crate::expr::physical::PhysicalSortExpression;

/// Extract sort keys from batches.
#[derive(Debug, Clone)]
pub struct SortKeysExtractor {
    /// Columns that make up the sort key.
    order_by: Vec<usize>,

    /// Encoder for producing comparable rows.
    encoder: ComparableRowEncoder,
}

impl SortKeysExtractor {
    pub fn new(exprs: &[PhysicalSortExpression]) -> Self {
        let order_by = exprs.iter().map(|expr| expr.column.idx).collect();
        let encoder = ComparableRowEncoder {
            columns: exprs
                .iter()
                .map(|expr| ComparableColumn {
                    desc: expr.desc,
                    nulls_first: expr.nulls_first,
                })
                .collect(),
        };

        SortKeysExtractor { order_by, encoder }
    }

    /// Get the sort keys for the batch as rows.
    pub fn sort_keys(&self, batch: &Batch) -> Result<ComparableRows> {
        let cols = self.sort_columns(batch)?;
        let rows = self.encoder.encode(&cols)?;
        Ok(rows)
    }

    /// Get the columns that make up the sort keys.
    pub fn sort_columns<'a>(&self, batch: &'a Batch) -> Result<Vec<&'a Array>> {
        let sort_cols = self
            .order_by
            .iter()
            .map(|idx| {
                batch
                    .column(*idx)
                    .map(|col| col.as_ref())
                    .ok_or_else(|| RayexecError::new("Missing column"))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(sort_cols)
    }
}
