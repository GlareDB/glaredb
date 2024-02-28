use std::fmt::Debug;
use std::hash::Hash;

use protogen::metastore::types::catalog::RuntimePreference;

use super::{
    DfLogicalPlan,
    ExtensionNode,
    UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA,
};
use crate::planner::physical_plan::remote_scan::ProviderReference;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Insert {
    pub source: DfLogicalPlan,
    pub provider: ProviderReference,
    pub runtime_preference: RuntimePreference,
}

impl UserDefinedLogicalNodeCore for Insert {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![&self.source]
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

impl ExtensionNode for Insert {
    const EXTENSION_NAME: &'static str = "Insert";
}
