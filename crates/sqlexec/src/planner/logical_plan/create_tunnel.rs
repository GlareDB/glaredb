use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]

pub struct CreateTunnel {
    pub name: String,
    pub if_not_exists: bool,
    pub options: TunnelOptions,
}
impl TryFrom<protogen::gen::metastore::service::CreateTunnel> for CreateTunnel {
    type Error = ProtoConvError;
    fn try_from(
        value: protogen::gen::metastore::service::CreateTunnel,
    ) -> Result<Self, Self::Error> {
        let options = value
            .options
            .ok_or(ProtoConvError::RequiredField("options".to_string()))?;

        Ok(Self {
            name: value.name,
            if_not_exists: value.if_not_exists,
            options: options.try_into()?,
        })
    }
}

impl UserDefinedLogicalNodeCore for CreateTunnel {
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
        write!(f, "CreateTunnel")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionType for CreateTunnel {
    const EXTENSION_NAME: &'static str = "CreateTunnel";
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("CreateTunnel::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use ::protogen::sqlexec::logical_plan::{LogicalPlanExtension, LogicalPlanExtensionType};
        use protogen::gen::metastore::service as protogen;

        let proto = protogen::CreateTunnel {
            name: self.name.clone(),
            options: Some(self.options.clone().into()),
            if_not_exists: self.if_not_exists,
        };

        let plan_type = LogicalPlanExtensionType::CreateTunnel(proto);

        let lp_extension = LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
