use protogen::metastore::types::service::AlterTableOperation;

use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterTable {
    pub schema: String,
    pub name: String,
    pub operation: AlterTableOperation,
}

impl UserDefinedLogicalNodeCore for AlterTable {
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

impl ExtensionNode for AlterTable {
    type ProtoRepr = protogen::gen::metastore::service::AlterTable;
    const EXTENSION_NAME: &'static str = "AlterTable";
}
