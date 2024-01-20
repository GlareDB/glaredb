use protogen::metastore::types::catalog::TableEntry;

use super::{
    DfLogicalPlan, Expr, ExtensionNode, UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Delete {
    pub table: TableEntry,
    pub where_expr: Option<Expr>,
}

impl UserDefinedLogicalNodeCore for Delete {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        Vec::new()
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        Vec::new()
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

impl ExtensionNode for Delete {
    const EXTENSION_NAME: &'static str = "Delete";
}
