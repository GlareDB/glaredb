use super::*;

#[derive(Clone, Debug)]
pub struct CreateTempTable {
    pub table_name: String,
    pub if_not_exists: bool,
    pub columns: Vec<Field>,
    pub source: Option<DfLogicalPlan>,
}
