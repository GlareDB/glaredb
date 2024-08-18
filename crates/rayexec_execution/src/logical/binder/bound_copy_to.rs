use crate::{database::DatabaseContext, functions::copy::CopyToFunction, proto::DatabaseProtoConv};
use rayexec_error::{OptionExt, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct BoundCopyTo {
    pub func: Box<dyn CopyToFunction>,
    // TODO: Args go here. I think.
}

impl DatabaseProtoConv for BoundCopyTo {
    type ProtoType = rayexec_proto::generated::binder::BoundCopyTo;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            func: Some(self.func.to_proto_ctx(context)?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            func: DatabaseProtoConv::from_proto_ctx(proto.func.required("func")?, context)?,
        })
    }
}
