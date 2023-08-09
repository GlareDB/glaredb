use std::sync::Arc;

use datafusion::{logical_expr::Extension, prelude::SessionContext};
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::planner::extension::DatafusionExtension;

pub mod exec;
pub mod planner;

#[derive(Debug)]
pub struct GlareDBExtensionCodec;

impl LogicalExtensionCodec for GlareDBExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[datafusion::logical_expr::LogicalPlan],
        ctx: &SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        let e = DatafusionExtension::try_decode(buf).unwrap();
        println!("e: {:?}", e);
        Ok(Extension { node: Arc::new(e) })
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        let e = DatafusionExtension::from_extension(node).unwrap();
        e.encode(buf).unwrap();
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        buf: &[u8],
        schema: datafusion::arrow::datatypes::SchemaRef,
        ctx: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        todo!()
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        todo!()
    }
}
