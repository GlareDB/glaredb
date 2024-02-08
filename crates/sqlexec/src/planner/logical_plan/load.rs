use datafusion::sql::sqlparser::ast::Ident;

use super::{
    DfLogicalPlan,
    ExtensionNode,
    UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA,
};

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Load {
    pub extension: Ident,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load")
            .field("extension", &self.extension)
            .finish()
    }
}

impl UserDefinedLogicalNodeCore for Load {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Load")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for Load {
    const EXTENSION_NAME: &'static str = "Load";
}
