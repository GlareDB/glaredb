use super::*;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ShowVariable {
    pub variable: String,
}

impl UserDefinedLogicalNodeCore for ShowVariable {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        &EMPTY_SCHEMA
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
    type ProtoRepr = protogen::sqlexec::logical_plan::ShowVariable;
    const EXTENSION_NAME: &'static str = "ShowVariable";

    fn try_decode(
        _proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        unimplemented!()
    }

    fn try_decode_extension(_extension: &LogicalPlanExtension) -> Result<Self> {
        unimplemented!()
    }

    fn try_encode(&self, _buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        unimplemented!()
    }
}
