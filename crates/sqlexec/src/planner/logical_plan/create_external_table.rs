use datafusion::arrow::datatypes::Schema;
use protogen::metastore::types::options::TableOptionsV1;

use super::{
    DfLogicalPlan,
    ExtensionNode,
    OwnedFullObjectReference,
    UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_LOGICAL_SCHEMA,
};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateExternalTable {
    pub tbl_reference: OwnedFullObjectReference,
    pub or_replace: bool,
    pub if_not_exists: bool,
    pub table_options: TableOptionsV1,
    pub tunnel: Option<String>,
    pub schema: Option<Schema>,
}

impl UserDefinedLogicalNodeCore for CreateExternalTable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::EXTENSION_NAME)
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateExternalTable {
    const EXTENSION_NAME: &'static str = "CreateExternalTable";
}
