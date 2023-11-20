use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateCredentials {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
    pub or_replace: bool,
}

impl UserDefinedLogicalNodeCore for CreateCredentials {
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
        write!(f, "CreateCredentials")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for CreateCredentials {
    type ProtoRepr = protogen::gen::metastore::service::CreateCredentials;
    const EXTENSION_NAME: &'static str = "CreateCredentials";

    fn try_decode(
        _: Self::ProtoRepr,
        _: &SessionContext,
        _: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        unimplemented!()
    }

    fn try_downcast_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("CreateCredentials::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, _: &mut Vec<u8>, _: &dyn LogicalExtensionCodec) -> Result<()> {
        unimplemented!()
    }
}
