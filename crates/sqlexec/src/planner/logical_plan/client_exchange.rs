use crate::remote::broadcast::exchange_exec::ClientExchangeInputSendExec;

use super::*;
use uuid::Uuid;

// TODO: Probably should be treated as solely physical.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ClientExchangeSend {
    pub broadcast_id: Uuid,
    pub input: DfLogicalPlan,
}

impl TryFrom<protogen::sqlexec::logical_plan::ClientExchangeSend> for ClientExchangeSend {
    type Error = ProtoConvError;

    fn try_from(
        proto: protogen::sqlexec::logical_plan::ClientExchangeSend,
    ) -> Result<Self, Self::Error> {
        unimplemented!()
    }
}

impl UserDefinedLogicalNodeCore for ClientExchangeSend {
    fn name(&self) -> &str {
        Self::EXTENSION_NAME
    }

    fn inputs(&self) -> Vec<&DfLogicalPlan> {
        vec![&self.input]
    }

    fn schema(&self) -> &datafusion::common::DFSchemaRef {
        // that's annoying
        unimplemented!()
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

impl ExtensionNode for ClientExchangeSend {
    const EXTENSION_NAME: &'static str = "ClientExchangeSend";

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "ClientExchange::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let client_exchange = protogen::ClientExchangeSend {
            broadcast_id: self.broadcast_id.into_bytes().to_vec(),
        };

        let extension = protogen::LogicalPlanExtensionType::ClientExchangeSend(client_exchange);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct ClientExchangeRecv {
    pub broadcast_id: Uuid,
}

impl TryFrom<protogen::sqlexec::logical_plan::ClientExchangeRecv> for ClientExchangeRecv {
    type Error = ProtoConvError;

    fn try_from(
        proto: protogen::sqlexec::logical_plan::ClientExchangeRecv,
    ) -> Result<Self, Self::Error> {
        let broadcast_id = Uuid::from_slice(&proto.broadcast_id)?;
        Ok(Self { broadcast_id })
    }
}

impl UserDefinedLogicalNodeCore for ClientExchangeRecv {
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

impl ExtensionNode for ClientExchangeRecv {
    const EXTENSION_NAME: &'static str = "ClientExchangeRecv";

    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self> {
        match extension.node.as_any().downcast_ref::<Self>() {
            Some(s) => Ok(s.clone()),
            None => Err(internal!(
                "ClientExchange::try_from_extension: unsupported extension",
            )),
        }
    }

    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()> {
        use protogen::sqlexec::logical_plan as protogen;

        let client_exchange = protogen::ClientExchangeRecv {
            broadcast_id: self.broadcast_id.into_bytes().to_vec(),
        };

        let extension = protogen::LogicalPlanExtensionType::ClientExchangeRecv(client_exchange);

        let lp_extension = protogen::LogicalPlanExtension {
            inner: Some(extension),
        };

        lp_extension
            .encode(buf)
            .map_err(|e| internal!("{}", e.to_string()))?;

        Ok(())
    }
}
