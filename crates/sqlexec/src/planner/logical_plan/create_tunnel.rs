use super::{DfLogicalPlan, ExtensionNode, GENERIC_OPERATION_LOGICAL_SCHEMA, TunnelOptions, UserDefinedLogicalNodeCore};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateTunnel {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TunnelOptions,
}

impl UserDefinedLogicalNodeCore for CreateTunnel {
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
        write!(f, "CreateTunnel")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateTunnel {
    const EXTENSION_NAME: &'static str = "CreateTunnel";
}
