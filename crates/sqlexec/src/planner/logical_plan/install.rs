use std::fmt::Debug;
use std::hash::Hash;

use super::{
    DfLogicalPlan,
    ExtensionNode,
    UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Install {
    pub extension: String,
}

impl UserDefinedLogicalNodeCore for Install {
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

impl ExtensionNode for Install {
    const EXTENSION_NAME: &'static str = "Install";
}
