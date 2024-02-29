use std::fmt::Debug;
use std::hash::Hash;

use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{DFSchemaRef, ToDFSchema};
use once_cell::sync::Lazy;

use super::{DfLogicalPlan, ExtensionNode, UserDefinedLogicalNodeCore};

static INSTALL_SCHEMA: Lazy<DFSchemaRef> = Lazy::new(|| {
    Schema::new(vec![Field::new("extension", DataType::Utf8, false)])
        .to_dfschema_ref()
        .unwrap()
});

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Install {
    pub extension: String,
}

impl UserDefinedLogicalNodeCore for Install {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &INSTALL_SCHEMA
    }

    fn expressions(&self) -> Vec<datafusion::prelude::Expr> {
        Vec::new()
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

impl ExtensionNode for Install {
    const EXTENSION_NAME: &'static str = "Install";
}
