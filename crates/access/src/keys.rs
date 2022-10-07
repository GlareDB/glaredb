//! Various cache key types.

// TODO: Schema id?
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BatchKey {
    /// The id for a table. Must be globally unique across all tables in the
    /// database.
    ///
    /// The partition id can be derived from the the file name:
    /// e.g. `/schema_1/table_<table_id>_part_<part_id>.data`
    pub table_id: u32,

    /// The partition id for a table. The id is unique amongst all partitions
    /// for a table. Partition ids do not indicate relative order of partitions.
    /// A higher level index structure is needed to determine the order of
    /// partitions.
    ///
    /// Similarly to `table_id`, the `part_id` can be derived from the file
    /// name.
    pub part_id: u32,

    /// The index of the batch within the partition.
    pub batch: u32,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
    pub table_id: u32,
    pub part_id: u32,
}
