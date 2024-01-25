use super::{DfLogicalPlan, ExtensionNode, GENERIC_OPERATION_LOGICAL_SCHEMA, UserDefinedLogicalNodeCore};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterTunnelRotateKeys {
    pub name: String,
    pub if_exists: bool,
    pub new_ssh_key: Vec<u8>,
}

impl UserDefinedLogicalNodeCore for AlterTunnelRotateKeys {
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

impl ExtensionNode for AlterTunnelRotateKeys {
    const EXTENSION_NAME: &'static str = "AlterTunnelRotateKeys";
}
