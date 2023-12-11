use datafusion::prelude::SessionContext;
use protogen::metastore::types::catalog::TableEntry;

use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Update {
    pub table: TableEntry,
    pub updates: Vec<(String, Expr)>,
    pub where_expr: Option<Expr>,
}

impl UserDefinedLogicalNodeCore for Update {
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

impl ExtensionNode for Update {
    type ProtoRepr = protogen::sqlexec::logical_plan::Update;
    const EXTENSION_NAME: &'static str = "Update";
}
