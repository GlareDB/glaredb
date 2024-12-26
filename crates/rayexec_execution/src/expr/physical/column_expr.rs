use std::borrow::Cow;
use std::fmt;

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use rayexec_error::{RayexecError, Result};

use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalColumnExpr {
    pub idx: usize,
}

impl PhysicalColumnExpr {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        let col = batch.column(self.idx).ok_or_else(|| {
            RayexecError::new(format!(
                "Tried to get column at index {} in a batch with {} columns",
                self.idx,
                batch.columns().len()
            ))
        })?;

        Ok(Cow::Borrowed(col))
    }
}

impl fmt::Display for PhysicalColumnExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.idx)
    }
}

impl DatabaseProtoConv for PhysicalColumnExpr {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalColumnExpr;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            idx: self.idx as u32,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            idx: proto.idx as usize,
        })
    }
}
