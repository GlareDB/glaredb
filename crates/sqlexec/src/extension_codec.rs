
use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::planner::extension::ExtensionConversion;
use crate::planner::logical_plan::CreateTable;

#[derive(Debug)]
pub struct GlareDBExtensionCodec;

impl LogicalExtensionCodec for GlareDBExtensionCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        // TODO: try decoding all known extensions
        todo!("try decoding all known extensions")
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        match node.node.name() {
            "CreateTable" => {
                let e = CreateTable::try_from_extension(node)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                e.encode(buf)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
            }
            _ => todo!("encode all known extensions"),
        }
        Ok(())
    }

    fn try_decode_table_provider(
        &self,
        _buf: &[u8],
        _schema: datafusion::arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        todo!()
    }

    fn try_encode_table_provider(
        &self,
        _node: Arc<dyn datafusion::datasource::TableProvider>,
        _buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        todo!()
    }
}
