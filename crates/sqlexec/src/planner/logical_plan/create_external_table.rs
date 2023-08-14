use super::*;

#[derive(Clone, Debug)]
pub struct CreateExternalTable {
    pub table_name: OwnedTableReference,
    pub if_not_exists: bool,
    pub table_options: TableOptions,
    pub tunnel: Option<String>,
}
