use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropViews {
    pub view_references: Vec<OwnedFullObjectReference>,
    pub if_exists: bool,
}

impl UserDefinedLogicalNodeCore for DropViews {
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
        write!(f, "DropViews")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropViews {
    type ProtoRepr = protogen::sqlexec::logical_plan::DropViews;
    const EXTENSION_NAME: &'static str = "DropViews";
}
