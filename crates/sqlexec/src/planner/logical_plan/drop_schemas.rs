use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropSchemas {
    pub schema_references: Vec<OwnedFullSchemaReference>,
    pub if_exists: bool,
    pub cascade: bool,
}

impl UserDefinedLogicalNodeCore for DropSchemas {
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
        write!(f, "DropSchemas")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropSchemas {
    const EXTENSION_NAME: &'static str = "DropSchemas";
}
