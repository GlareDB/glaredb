use std::sync::Arc;

use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;

use crate::planner::extension::ExtensionConversion;
use crate::planner::logical_plan::CreateTable;
use protogen::export::prost::Message;

#[derive(Debug)]
pub struct GlareDBExtensionCodec;

impl LogicalExtensionCodec for GlareDBExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        _inputs: &[datafusion::logical_expr::LogicalPlan],
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<datafusion::logical_expr::Extension> {
        let lp_extension = protogen::sqlexec::logical_plan::LogicalPlanExtension::decode(buf)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        match lp_extension.inner.unwrap() {
            protogen::sqlexec::logical_plan::LogicalPlanExtensionType::DdlPlan(node) => {
                match node.ddl.unwrap() {
                    protogen::sqlexec::logical_plan::DdlPlanType::CreateTable(create_table) => {
                        let e = CreateTable::try_from(create_table)
                            .map_err(|e| DataFusionError::External(Box::new(e)))?;
                        Ok(e.into_extension())
                    }
                    _ => todo!("try decoding all known ddl plans"),
                }
            }
        }
    }

    fn try_encode(
        &self,
        node: &datafusion::logical_expr::Extension,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        match node.node.name() {
            "CreateTable" => {
                println!("encoding CreateTable");
                let e = CreateTable::try_from_extension(node)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                e.encode(buf, self)
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
