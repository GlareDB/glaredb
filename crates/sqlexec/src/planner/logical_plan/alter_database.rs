use protogen::metastore::types::service::AlterDatabaseOperation;

use super::{DfLogicalPlan, ExtensionNode, GENERIC_OPERATION_LOGICAL_SCHEMA, UserDefinedLogicalNodeCore};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterDatabase {
    pub name: String,
    pub operation: AlterDatabaseOperation,
}

impl UserDefinedLogicalNodeCore for AlterDatabase {
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

impl ExtensionNode for AlterDatabase {
    const EXTENSION_NAME: &'static str = "AlterDatabase";
}
