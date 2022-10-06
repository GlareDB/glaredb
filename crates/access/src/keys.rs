//! Various cache key types.

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BatchKey {
    pub table_id: u32,
    pub part_id: u32,
    pub batch: u32,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
    pub table_id: u32,
    pub part_id: u32,
}
