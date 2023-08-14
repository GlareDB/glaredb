use super::*;

#[derive(Clone, Debug)]
pub struct DropViews {
    pub names: Vec<OwnedTableReference>,
    pub if_exists: bool,
}
