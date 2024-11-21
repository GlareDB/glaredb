use rayexec_error::Result;
use rayexec_execution::storage::table_storage::{DataTable, DataTableScan, Projections};

#[derive(Debug)]
pub struct IcebergDataTable {}

impl DataTable for IcebergDataTable {
    fn scan(
        &self,
        projections: Projections,
        num_partitions: usize,
    ) -> Result<Vec<Box<dyn DataTableScan>>> {
        unimplemented!()
    }
}
