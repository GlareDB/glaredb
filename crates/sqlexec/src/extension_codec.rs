use core::fmt;
use std::collections::HashMap;
use std::sync::Arc;

use datafusion::datasource::TableProvider;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use datafusion_proto::logical_plan::LogicalExtensionCodec;
use uuid::Uuid;

use crate::errors::ExecError;
use crate::planner::extension::ExtensionConversion;
use crate::planner::logical_plan::CreateTable;
use protogen::export::prost::Message;
use crate::remote::table::RemoteTableProvider;

pub struct GlareDBExtensionCodec<'a> {
    table_providers: Option<&'a HashMap<Uuid, Arc<dyn TableProvider>>>,
}

impl<'a> GlareDBExtensionCodec<'a> {
    pub fn new_decoder(table_providers: &'a HashMap<Uuid, Arc<dyn TableProvider>>) -> Self {
        Self {
            table_providers: Some(table_providers),
        }
    }

    pub fn new_encoder() -> Self {
        Self {
            table_providers: None,
        }
    }
}

impl<'a> fmt::Debug for GlareDBExtensionCodec<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GlareDBExtensionCodec").finish()
    }
}

impl<'a> LogicalExtensionCodec for GlareDBExtensionCodec<'a> {
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
        buf: &[u8],
        _schema: datafusion::arrow::datatypes::SchemaRef,
        _ctx: &SessionContext,
    ) -> datafusion::error::Result<Arc<dyn datafusion::datasource::TableProvider>> {
        let provider_id = Uuid::from_slice(buf).map_err(|e| {
            DataFusionError::External(Box::new(ExecError::InvalidRemoteId("provider", e)))
        })?;

        let provider = self
            .table_providers
            .and_then(|table_providers| table_providers.get(&provider_id))
            .ok_or_else(|| {
                DataFusionError::External(Box::new(ExecError::Internal(format!(
                    "cannot decode the table provider for id: {provider_id}"
                ))))
            })?;

        Ok(Arc::clone(provider))
    }

    fn try_encode_table_provider(
        &self,
        node: Arc<dyn datafusion::datasource::TableProvider>,
        buf: &mut Vec<u8>,
    ) -> datafusion::error::Result<()> {
        if let Some(remote_provider) = node.as_any().downcast_ref::<RemoteTableProvider>() {
            remote_provider
                .encode(buf)
                .map_err(|e| DataFusionError::External(Box::new(e)))
        } else {
            Err(DataFusionError::External(Box::new(ExecError::Internal(
                "can only encode `RemoteTableProvider`".to_string(),
            ))))
        }
    }
}
