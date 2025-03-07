use std::fmt::Debug;

use rayexec_error::Result;

use crate::execution::operators::PlannedOperatorWithChildren;
use crate::logical::logical_create::LogicalCreateTable;
use crate::logical::logical_insert::LogicalInsert;

pub trait StorageManager: Debug + Sync + Send {
    fn plan_insert(
        &self,
        insert: &LogicalInsert,
        input: PlannedOperatorWithChildren,
    ) -> Result<PlannedOperatorWithChildren>;

    fn plan_create_table_as(
        &self,
        create: &LogicalCreateTable,
        input: PlannedOperatorWithChildren,
    ) -> Result<PlannedOperatorWithChildren>;
}
