/// extension implementations for converting our logical plan into datafusion logical plan
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use std::sync::Arc;

use crate::{errors::Result, LogicalPlan};
use datafusion::logical_expr::{Extension as LogicalPlanExtension, UserDefinedLogicalNodeCore};

pub trait ExtensionType: Sized + UserDefinedLogicalNodeCore {
    const EXTENSION_NAME: &'static str;
    fn into_extension(self) -> LogicalPlanExtension {
        LogicalPlanExtension {
            node: Arc::new(self),
        }
    }
    fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan::Datafusion(datafusion::logical_expr::LogicalPlan::Extension(
            self.into_extension(),
        ))
    }
    fn try_decode_extension(extension: &LogicalPlanExtension) -> Result<Self>;
    fn try_encode(&self, buf: &mut Vec<u8>, _codec: &dyn LogicalExtensionCodec) -> Result<()>;
    fn try_encode_extension(
        extension: &LogicalPlanExtension,
        buf: &mut Vec<u8>,
        codec: &dyn LogicalExtensionCodec,
    ) -> Result<()> {
        let extension = Self::try_decode_extension(extension)?;
        extension.try_encode(buf, codec)
    }
}
