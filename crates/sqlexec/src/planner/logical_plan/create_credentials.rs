use super::{
    CredentialsOptions, DfLogicalPlan, ExtensionNode, UserDefinedLogicalNodeCore,
    GENERIC_OPERATION_LOGICAL_SCHEMA,
};
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateCredentials {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
    pub or_replace: bool,
}

impl UserDefinedLogicalNodeCore for CreateCredentials {
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
        write!(f, "CreateCredentials")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateCredentials {
    const EXTENSION_NAME: &'static str = "CreateCredentials";
}
