use crate::{
    database::{catalog::CatalogTx, create::CreateTableInfo, DatabaseContext},
    logical::explainable::{ExplainConfig, ExplainEntry, Explainable},
    proto::DatabaseProtoConv,
    storage::table_storage::DataTable,
};
use futures::future::BoxFuture;
use rayexec_bullet::batch::Batch;
use rayexec_error::{OptionExt, RayexecError, Result};
use rayexec_proto::ProtoConv;
use std::fmt;

use super::{
    sink::{PartitionSink, SinkOperation, SinkOperator},
    util::barrier::PartitionBarrier,
};

pub type PhysicalCreateTable = SinkOperator<CreateTableSinkOperation>;

#[derive(Debug)]
pub struct CreateTableSinkOperation {
    pub catalog: String,
    pub schema: String,
    pub info: CreateTableInfo,
    pub is_ctas: bool,
}

impl SinkOperation for CreateTableSinkOperation {
    fn create_partition_sinks(
        &self,
        context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        // TODO: Placeholder.
        let tx = CatalogTx::new();

        let database = context.get_database(&self.catalog)?;
        let table_storage = database
            .table_storage
            .as_ref()
            .ok_or_else(|| {
                RayexecError::new(
                    "Missing table storage, cannot create a table inside this database",
                )
            })?
            .clone();

        let schema_ent = database
            .catalog
            .get_schema(&tx, &self.schema)?
            .ok_or_else(|| {
                RayexecError::new(format!("Missing schema for table create: {}", self.schema))
            })?;

        let info = self.info.clone();

        let create_table_fut = Box::pin(async move {
            let table_ent = schema_ent.create_table(&tx, &info)?;
            let datatable = table_storage
                .create_physical_table(&schema_ent.entry().name, &table_ent)
                .await?;

            Ok(datatable)
        });

        let insert_barrier = PartitionBarrier::new(num_sinks);

        // First partition is responsible for actually creating the table.
        let mut sinks = vec![Box::new(CreateTablePartitionSink {
            is_ctas: self.is_ctas,
            num_partitions: num_sinks,
            partition_idx: 0,
            create_table_fut: Some(create_table_fut),
            insert_barrier: insert_barrier.clone(),
            sink: None,
        }) as _];

        sinks.extend((1..num_sinks).map(|idx| {
            Box::new(CreateTablePartitionSink {
                is_ctas: self.is_ctas,
                num_partitions: num_sinks,
                partition_idx: idx,
                create_table_fut: None,
                insert_barrier: insert_barrier.clone(),
                sink: None,
            }) as _
        }));

        Ok(sinks)
    }

    fn partition_requirement(&self) -> Option<usize> {
        None
    }
}

impl Explainable for CreateTableSinkOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CreateTable").with_value("table", &self.info.name)
    }
}

struct CreateTablePartitionSink {
    is_ctas: bool,
    num_partitions: usize,
    partition_idx: usize,

    /// Optional future for creating the table.
    ///
    /// This will only be set for one partition. If None, shared state should be
    /// checked to get the appropriate sinks if needed.
    create_table_fut: Option<BoxFuture<'static, Result<Box<dyn DataTable>>>>,

    /// Barrier stopping partitions from trying to insert prior to creating the
    /// table.
    insert_barrier: PartitionBarrier<Box<dyn PartitionSink>>,

    /// Sink this partition is pushing batches to, if any.
    sink: Option<Box<dyn PartitionSink>>,
}

impl PartitionSink for CreateTablePartitionSink {
    fn push(&mut self, batch: Batch) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.create_table_if_has_fut().await?;
            self.wait_for_sink_if_none().await;

            if let Some(sink) = &mut self.sink {
                sink.push(batch).await?;
            }

            Ok(())
        })
    }

    fn finalize(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async {
            self.create_table_if_has_fut().await?;
            self.wait_for_sink_if_none().await;

            if let Some(sink) = &mut self.sink {
                sink.finalize().await?;
            }

            Ok(())
        })
    }
}

impl CreateTablePartitionSink {
    /// Creates the table using the stored create table future if this partition
    /// has it.
    ///
    /// If this partition has the future, it will generate the appropriate
    /// partition sinks for all partitions, and unblock the `insert_barrier`
    /// allow other partitions to start inserting into the table (CTAS only).
    ///
    /// If the partition is not responsible for creating the table, it will be
    /// blocked until the `insert_barrier` is unblocked (for both CTAS and
    /// non-CTAS).
    async fn create_table_if_has_fut(&mut self) -> Result<()> {
        if let Some(create_fut) = self.create_table_fut.take() {
            let table = create_fut.await?;

            if self.is_ctas {
                let sinks = table.insert(self.num_partitions)?;
                self.insert_barrier
                    .unblock(sinks.into_iter().map(Some).collect());
            } else {
                self.insert_barrier
                    .unblock((0..self.num_partitions).map(|_| None).collect());
            }
        }
        Ok(())
    }

    async fn wait_for_sink_if_none(&mut self) {
        if self.sink.is_none() {
            self.sink = self
                .insert_barrier
                .item_for_partition(self.partition_idx)
                .await;
        }
    }
}

impl fmt::Debug for CreateTablePartitionSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateTablePartitionSink")
            .finish_non_exhaustive()
    }
}

impl DatabaseProtoConv for PhysicalCreateTable {
    type ProtoType = rayexec_proto::generated::execution::PhysicalCreateTable;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            catalog: self.sink.catalog.clone(),
            schema: self.sink.schema.clone(),
            info: Some(self.sink.info.to_proto()?),
            is_ctas: self.sink.is_ctas,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(SinkOperator::new(CreateTableSinkOperation {
            catalog: proto.catalog,
            schema: proto.schema,
            info: CreateTableInfo::from_proto(proto.info.required("info")?)?,
            is_ctas: proto.is_ctas,
        }))
    }
}
