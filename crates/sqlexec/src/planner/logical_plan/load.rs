use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DFSchemaRef, ToDFSchema};
use datafusion::sql::sqlparser::ast::Ident;
use once_cell::sync::Lazy;

use super::{DfLogicalPlan, ExtensionNode, UserDefinedLogicalNodeCore};

#[derive(Clone, Hash, PartialEq, Eq)]
pub struct Load {
    pub extension: String,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load")
            .field("extension", &self.extension)
            .finish()
    }
}

pub static LOAD_SCHEMA: Lazy<DFSchemaRef> = Lazy::new(|| {
    Schema::new(vec![
        Field::new(
            "extension",
            datafusion::arrow::datatypes::DataType::Utf8,
            false,
        ),
        Field::new(
            "loaded",
            datafusion::arrow::datatypes::DataType::Boolean,
            false,
        ),
        Field::new(
            "remote",
            datafusion::arrow::datatypes::DataType::Boolean,
            false,
        ),
    ])
    .to_dfschema_ref()
    .unwrap()
});

impl UserDefinedLogicalNodeCore for Load {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &LOAD_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Load")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for Load {
    const EXTENSION_NAME: &'static str = "Load";
}
