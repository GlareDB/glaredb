use super::*;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct AlterTunnelRotateKeys {
    pub name: String,
    pub if_exists: bool,
    pub new_ssh_key: Vec<u8>,
}

impl TryFrom<protogen::gen::metastore::service::AlterTunnelRotateKeys> for AlterTunnelRotateKeys {
    type Error = ProtoConvError;

    fn try_from(
        proto: protogen::gen::metastore::service::AlterTunnelRotateKeys,
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            name: proto.name,
            if_exists: proto.if_exists,
            new_ssh_key: proto.new_ssh_key,
        })
    }
}


impl UserDefinedLogicalNodeCore for AlterTunnelRotateKeys {
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

impl ExtensionType for AlterTunnelRotateKeys {
    const EXTENSION_NAME: &'static str = "AlterTunnelRotateKeys";

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "AlterTunnelRotateKeys decode failed",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;
        let Self { name, if_exists, new_ssh_key } = self.clone();

        let alter_tunnel_rotate_keys =
            ::protogen::gen::metastore::service::AlterTunnelRotateKeys { name, if_exists, new_ssh_key };

        let extension = protogen::LogicalPlanExtensionType::AlterTunnelRotateKeys(alter_tunnel_rotate_keys);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
