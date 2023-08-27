use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CreateCredentials {
    pub name: String,
    pub options: CredentialsOptions,
    pub comment: String,
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
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        let options = proto
            .options
            .ok_or(ProtoConvError::RequiredField("options".to_string()))?;

        Ok(Self {
            name: proto.name,
            options: options.try_into()?,
            comment: proto.comment,
        })
    }

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("CreateCredentials::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use ::protogen::sqlexec::logical_plan::{LogicalPlanExtension, LogicalPlanExtensionType};

        use protogen::gen::metastore::service as protogen;
        let Self {
            name,
            options,
            comment,
        } = self.clone();

        let proto = protogen::CreateCredentials {
            name,
            options: Some(options.into()),
            comment,
        };
        let plan_type = LogicalPlanExtensionType::CreateCredentials(proto);

        let lp_extension = LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
