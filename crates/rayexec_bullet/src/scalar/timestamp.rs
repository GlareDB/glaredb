use rayexec_error::Result;
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::datatype::TimeUnit;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimestampScalar {
    pub unit: TimeUnit,
    pub value: i64,
}

impl ProtoConv for TimestampScalar {
    type ProtoType = rayexec_proto::generated::expr::TimestampScalar;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            unit: self.unit.to_proto()? as i32,
            value: self.value,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            unit: TimeUnit::from_proto(proto.unit())?,
            value: proto.value,
        })
    }
}
