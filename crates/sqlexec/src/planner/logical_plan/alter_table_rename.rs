use super::*;
#[derive(Clone, Debug)]
pub struct AlterTableRename {
    pub name: OwnedTableReference,
    pub new_name: OwnedTableReference,
}
