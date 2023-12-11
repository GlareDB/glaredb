use super::*;

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
        write!(
            f,
            "CREATE SCHEMA{if_not_exists} {schema_reference}",
            if_not_exists = if self.if_not_exists {
                " IF NOT EXISTS"
            } else {
                ""
            },
            schema_reference = self.schema_reference
        )
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
