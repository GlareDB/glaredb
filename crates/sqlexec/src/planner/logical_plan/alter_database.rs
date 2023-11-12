use datafusion::prelude::SessionContext;
use protogen::metastore::types::service::AlterDatabaseOperation;

use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterDatabase {
    pub name: String,
    pub operation: AlterDatabaseOperation,
}

impl UserDefinedLogicalNodeCore for AlterDatabase {
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

impl ExtensionNode for AlterDatabase {
    type ProtoRepr = protogen::gen::metastore::service::AlterDatabase;
    const EXTENSION_NAME: &'static str = "AlterDatabase";
    fn try_decode(
        _proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        unimplemented!()
    }

    fn try_downcast_extension(_extension: &LogicalPlanExtension) -> Result<Self> {
        unimplemented!()
    }

    fn try_encode(&self, _buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        unimplemented!()
    }
}
