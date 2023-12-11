use super::*;

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct CopyTo {
    pub source: DfLogicalPlan,
    pub dest: CopyToDestinationOptions,
    pub format: CopyToFormatOptions,
}

impl std::fmt::Debug for CopyTo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CopyTo")
            .field("source", &self.source.schema())
            .field("dest", &self.dest)
            .field("format", &self.format)
            .finish()
    }
}

impl UserDefinedLogicalNodeCore for CopyTo {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![&self.source]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &GENERIC_OPERATION_AND_COUNT_LOGICAL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "CopyTo")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CopyTo {
    const EXTENSION_NAME: &'static str = "CopyTo";
}
