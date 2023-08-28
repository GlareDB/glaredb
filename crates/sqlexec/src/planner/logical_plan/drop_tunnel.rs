use super::*;
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DropTunnel {
    pub names: Vec<String>,
    pub if_exists: bool,
}

impl UserDefinedLogicalNodeCore for DropTunnel {
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
        write!(f, "DropTunnel")
    }

    fn from_template(
        &self,
        _exprs: &[datafusion::prelude::Expr],
        _inputs: &[DfLogicalPlan],
    ) -> Self {
        self.clone()
    }
}

impl ExtensionNode for DropTunnel {
    type ProtoRepr = protogen::sqlexec::logical_plan::DropTunnel;
    const EXTENSION_NAME: &'static str = "DropTunnel";
    fn try_decode(
        proto: Self::ProtoRepr,
        _ctx: &SessionContext,
        _codec: &dyn LogicalExtensionCodec,
    ) -> std::result::Result<Self, ProtoConvError> {
        Ok(Self {
            names: proto.names,
            if_exists: proto.if_exists,
        })
    }
    fn try_downcast_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!("DropTunnel::try_decode_extension failed",)),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use ::protogen::sqlexec::logical_plan::{
            self as protogen, LogicalPlanExtension, LogicalPlanExtensionType,
        };

        let proto = protogen::DropTunnel {
            names: self.names.clone(),
            if_exists: self.if_exists,
        };

        let plan_type = LogicalPlanExtensionType::DropTunnel(proto);

        let lp_extension = LogicalPlanExtension {
            inner: Some(plan_type),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
