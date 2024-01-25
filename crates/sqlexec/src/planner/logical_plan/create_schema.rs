use super::{DfLogicalPlan, ExtensionNode, GENERIC_OPERATION_LOGICAL_SCHEMA, OwnedFullSchemaReference, UserDefinedLogicalNodeCore};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateSchema {
    pub schema_reference: OwnedFullSchemaReference,
    pub if_not_exists: bool,
}

impl UserDefinedLogicalNodeCore for CreateSchema {
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
        write!(f, "CreateSchema")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateSchema {
    const EXTENSION_NAME: &'static str = "CreateSchema";
}
