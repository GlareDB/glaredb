use protogen::metastore::types::options::TableOptionsImpl;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableOptionsDebug {
    pub table_type: String,
}

impl TableOptionsImpl for TableOptionsDebug {
    fn name(&self) -> &'static str {
        "debug"
    }
}
