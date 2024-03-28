use super::{
    DfLogicalPlan,
    ExtensionNode,
    UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_LOGICAL_SCHEMA,
};
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropCredentials {
    pub names: Vec<String>,
    pub if_exists: bool,
    /// If true, show a deprecation warning when the operation is executed.
    pub show_deprecation_warning: bool,
}

impl UserDefinedLogicalNodeCore for DropCredentials {
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
        write!(f, "DropCredentials")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropCredentials {
    const EXTENSION_NAME: &'static str = "DropCredentials";
}
