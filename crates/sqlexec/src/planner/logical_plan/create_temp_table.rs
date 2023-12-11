use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateTempTable {
    pub tbl_reference: OwnedFullObjectReference,
    pub if_not_exists: bool,
    pub or_replace: bool,
    pub schema: DFSchemaRef,
    pub source: Option<DfLogicalPlan>,
}

impl UserDefinedLogicalNodeCore for CreateTempTable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        match self.source {
            Some(ref src) => vec![src],
            None => vec![],
        }
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

impl ExtensionNode for CreateTempTable {
    type ProtoRepr = protogen::sqlexec::logical_plan::CreateTempTable;
    const EXTENSION_NAME: &'static str = "CreateTempTable";
}
