use super::{DatabaseOptions, DfLogicalPlan, ExtensionNode, GENERIC_OPERATION_LOGICAL_SCHEMA, UserDefinedLogicalNodeCore};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateExternalDatabase {
    pub database_name: String,
    pub if_not_exists: bool,
    pub options: DatabaseOptions,
    pub tunnel: Option<String>,
}

impl UserDefinedLogicalNodeCore for CreateExternalDatabase {
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
        write!(f, "CreateExternalDatabase")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateExternalDatabase {
    const EXTENSION_NAME: &'static str = "CreateExternalDatabase";
}
