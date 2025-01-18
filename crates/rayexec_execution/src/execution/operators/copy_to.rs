use rayexec_error::{OptionExt, Result};
use rayexec_io::location::FileLocation;
use rayexec_proto::ProtoConv;

use super::sink::operation::{PartitionSink, SinkOperation};
use super::sink::PhysicalSink;
use crate::arrays::field::Schema;
use crate::database::DatabaseContext;
use crate::explain::explainable::{ExplainConfig, ExplainEntry, Explainable};
use crate::functions::copy::CopyToFunction;
use crate::proto::DatabaseProtoConv;

pub type PhysicalCopyTo = PhysicalSink<CopyToOperation>;

#[derive(Debug)]
pub struct CopyToOperation {
    pub copy_to: Box<dyn CopyToFunction>,
    pub location: FileLocation,
    pub schema: Schema,
}

impl SinkOperation for CopyToOperation {
    fn create_partition_sinks(
        &self,
        _context: &DatabaseContext,
        num_sinks: usize,
    ) -> Result<Vec<Box<dyn PartitionSink>>> {
        self.copy_to
            .create_sinks(self.schema.clone(), self.location.clone(), num_sinks)
    }

    fn partitioning_requirement(&self) -> Option<usize> {
        // TODO: Until we figure out partitioned COPY TO.
        Some(1)
    }
}

impl Explainable for CopyToOperation {
    fn explain_entry(&self, _conf: ExplainConfig) -> ExplainEntry {
        ExplainEntry::new("CopyTo").with_value("location", &self.location)
    }
}

impl DatabaseProtoConv for PhysicalCopyTo {
    type ProtoType = rayexec_proto::generated::execution::PhysicalCopyTo;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            copy_to: Some(self.sink.copy_to.to_proto_ctx(context)?),
            location: Some(self.sink.location.to_proto()?),
            schema: Some(self.sink.schema.to_proto()?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(PhysicalSink::new(CopyToOperation {
            copy_to: DatabaseProtoConv::from_proto_ctx(
                proto.copy_to.required("copy_to")?,
                context,
            )?,
            location: ProtoConv::from_proto(proto.location.required("location")?)?,
            schema: ProtoConv::from_proto(proto.schema.required("schema")?)?,
        }))
    }
}
