use std::borrow::Cow;
use std::fmt;

use rayexec_error::{RayexecError, Result};

use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalColumnExpr {
    pub idx: usize,
    pub datatype: DataType,
}

impl PhysicalColumnExpr {
    pub fn eval<'a>(&self, batch: &'a Batch) -> Result<Cow<'a, Array>> {
        let col = batch.array(self.idx).ok_or_else(|| {
            RayexecError::new(format!(
                "Tried to get column at index {} in a batch with {} columns",
                self.idx,
                batch.arrays().len()
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
        unimplemented!()
        // Ok(Self::ProtoType {
        //     idx: self.idx as u32,
        // })
    }

    fn from_proto_ctx(_proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        unimplemented!()
        // Ok(Self {
        //     idx: proto.idx as usize,
        // })
    }
}
