use crate::{
    database::{catalog::CatalogTx, catalog_entry::CatalogEntry, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    proto::DatabaseProtoConv,
};
use rayexec_error::{OptionExt, RayexecError, Result};
use std::sync::Arc;

use super::sink::{PartitionSink, SinkOperation, SinkOperator};

pub type PhysicalInsert = SinkOperator<InsertOperation>;

#[derive(Debug)]
pub struct InsertOperation {
    pub catalog: String,
    pub schema: String,
    pub table: Arc<CatalogEntry>,
}

impl SinkOperation for InsertOperation {
    fn create_partition_sinks(
        &self,
        context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        // TODO: Placeholder.
        let _tx = CatalogTx::new();

        let database = context.get_database(&self.catalog)?;
        let data_table = database
            .table_storage
            .as_ref()
            .ok_or_else(|| RayexecError::new("Missing table storage for insert"))?
            .data_table(&self.schema, &self.table)?;

        // TODO: Pass constraints, on conflict
        let inserts = data_table.insert(num_sinks)?;

        Ok(inserts)
    }

    fn partition_requirement(&self) -> Option<usize> {
        None
    }
}

impl Explainable for InsertOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("Insert").with_value("table", &self.table.name)
    }
}

impl DatabaseProtoConv for PhysicalInsert {
    type ProtoType = rayexec_proto::generated::execution::PhysicalInsert;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            catalog: self.sink.catalog.clone(),
            schema: self.sink.schema.clone(),
            table: Some(self.sink.table.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(SinkOperator::new(InsertOperation {
            catalog: proto.catalog,
            schema: proto.schema,
            table: Arc::new(DatabaseProtoConv::from_proto_ctx(
                proto.table.required("table")?,
                context,
            )?),
        }))
    }
}
