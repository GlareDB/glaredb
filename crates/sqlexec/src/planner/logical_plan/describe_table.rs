use super::*;
use datafusion::{
    arrow::datatypes::{Field, Schema, SchemaRef},
    common::ToDFSchema,
};
use protogen::metastore::types::catalog::TableEntry;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DescribeTable {
    pub entry: TableEntry,
}

pub static DESCRIBE_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("column_name", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("is_nullable", DataType::Boolean, false),
    ]))
});

pub static DESCRIBE_TABLE_LOGICAL_SCHEMA: Lazy<DFSchemaRef> =
    Lazy::new(|| DESCRIBE_TABLE_SCHEMA.clone().to_dfschema_ref().unwrap());

impl UserDefinedLogicalNodeCore for DescribeTable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &DESCRIBE_TABLE_LOGICAL_SCHEMA
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

impl ExtensionNode for DescribeTable {
    const EXTENSION_NAME: &'static str = "DescribeTable";
}
