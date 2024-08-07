use rayexec_error::Result;
use rayexec_proto::ProtoConv;

use crate::database::DatabaseContext;

/// Convert types to/from their protobuf representations with access to the
/// database context.
pub trait DatabaseProtoConv: Sized {
    type ProtoType;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType>;
    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self>;
}

/// Default implementation for anything implementing the stateless proto
/// conversion trait.
///
/// The database context that's provide is just ignored, and the underlying
/// to/from methods are called.
#[derive(Debug)]
pub struct WrappedProtoConv<P: ProtoConv>(pub P);

impl<P: ProtoConv> DatabaseProtoConv for WrappedProtoConv<P> {
    type ProtoType = P::ProtoType;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        self.0.to_proto()
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self(P::from_proto(proto)?))
    }
}

impl<P: ProtoConv> From<P> for WrappedProtoConv<P> {
    fn from(value: P) -> Self {
        Self(value)
    }
}
