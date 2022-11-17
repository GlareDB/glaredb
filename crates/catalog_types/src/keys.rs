//! Various key types.
use object_store::path::Path as ObjectPath;
use std::fmt;

pub type SchemaId = u32;
pub type TableId = u32;
pub type PartitionId = u32;
pub type BatchId = u32;

// TODO: Determine what needs to be globally unique.

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct BatchKey {
    /// The id of the schema.
    pub schema_id: SchemaId,

    /// The id for a table. Must be globally unique across all tables in the
    /// database.
    ///
    /// The partition id can be derived from the the file name:
    /// e.g. `/schema_1/table_<table_id>_part_<part_id>.data`
    pub table_id: TableId,

    /// The partition id for a table. The id is unique amongst all partitions
    /// for a table. Partition ids do not indicate relative order of partitions.
    /// A higher level index structure is needed to determine the order of
    /// partitions.
    ///
    /// Similarly to `table_id`, the `part_id` can be derived from the file
    /// name.
    pub part_id: PartitionId,

    /// The index of the batch within the partition.
    pub batch: BatchId,
}

impl fmt::Display for BatchKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BatchKey: schema={}, table={}, partition={}, batch={}",
            self.schema_id, self.table_id, self.part_id, self.batch
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionKey {
    pub schema_id: SchemaId,
    pub table_id: TableId,
    pub part_id: PartitionId,
}

impl PartitionKey {
    /// Get the object path for a partition.
    pub fn object_path(&self, db_name: &str) -> ObjectPath {
        let path = format!(
            "{}/schema_{}_table_{}_part_{}.data",
            db_name, self.schema_id, self.table_id, self.part_id
        );
        ObjectPath::from(path)
    }
}

impl fmt::Display for PartitionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PartitionKey: schema={}, table={}, partition={}",
            self.schema_id, self.table_id, self.part_id
        )
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableKey {
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

impl TableKey {
    pub fn partition_key(&self, part_id: PartitionId) -> PartitionKey {
        PartitionKey {
            schema_id: self.schema_id,
            table_id: self.table_id,
            part_id,
        }
    }
}

impl fmt::Display for TableKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "TableKey: schema={}, table={}",
            self.schema_id, self.table_id,
        )
    }
}
