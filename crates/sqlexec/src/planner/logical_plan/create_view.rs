use super::*;
#[derive(Clone, Debug)]
pub struct CreateView {
    pub view_name: OwnedTableReference,
    pub sql: String,
    pub columns: Vec<String>,
    pub or_replace: bool,
}
