use super::{
    Arc, DFField, DFSchema, DFSchemaRef, DataType, DfLogicalPlan, ExtensionNode, HashMap,
    UserDefinedLogicalNodeCore,
};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShowVariable {
    pub variable: String,
    /// Schema of the logical plan output. Note that this is hacky, but this is
    /// needed to return a reference to the Arc, and the schema needs to be dynamic.
    pub df_schema: DFSchemaRef,
}

impl ShowVariable {
    pub fn new(variable: String) -> ShowVariable {
        let df_schema = Arc::new(
            DFSchema::new_with_metadata(
                vec![DFField::new_unqualified(&variable, DataType::Utf8, false)],
                HashMap::new(),
            )
            .expect("creating df schema for SHOW should not fail"),
        );
        ShowVariable {
            variable,
            df_schema,
        }
    }
}

impl UserDefinedLogicalNodeCore for ShowVariable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &self.df_schema
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "ShowVariable")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for ShowVariable {
    const EXTENSION_NAME: &'static str = "ShowVariable";
}
