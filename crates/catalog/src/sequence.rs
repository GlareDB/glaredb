use crate::errors::Result;
use crate::system::SystemTableAccessor;
use access::runtime::AccessRuntime;
use catalog_types::keys::{SchemaId, TableId};
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequenceIdentifier {
    pub schema: SchemaId,
    pub table: TableId,
}

pub struct SequenceObject<'a, T> {
    seq_id: SequenceIdentifier,
    sequences_table: &'a T,
}

impl<'a, T: SystemTableAccessor> SequenceObject<'a, T> {
    /// Create a new sequence object for some table.
    pub fn new(seq_id: SequenceIdentifier, sequences_table: &'a T) -> Self {
        // TODO: Cache sequence values.
        // TODO: Check that sequence/table actually exists.
        SequenceObject {
            seq_id,
            sequences_table,
        }
    }

    /// Get the next value in the sequence.
    // NOTE: Will likely lack the ability rollback with transaction aborts,
    // resulting in gaps.
    pub async fn next_val(&self, runtime: Arc<AccessRuntime>) -> Result<u64> {
        // Execute scan+insert.
        // Need execution context.
        // Eventually want in place updates for tables.
        unimplemented!()
    }
}
