use std::fmt;
use std::sync::Arc;

use rayexec_bullet::{array::Array, batch::Batch, bitmap::Bitmap, compute};
use rayexec_error::{RayexecError, Result};

use crate::{database::DatabaseContext, proto::DatabaseProtoConv};

#[derive(Debug, Clone)]
pub struct PhysicalColumnExpr {
    pub idx: usize,
}

impl PhysicalColumnExpr {
    pub fn eval(&self, batch: &Batch, selection: Option<&Bitmap>) -> Result<Arc<Array>> {
        let col = batch.column(self.idx).ok_or_else(|| {
            RayexecError::new(format!(
                "Tried to get column at index {} in a batch with {} columns",
                self.idx,
                batch.columns().len()
            ))
        })?;

        match selection {
            Some(selection) => {
                let arr = compute::filter::filter(col.as_ref(), selection)?;
                Ok(Arc::new(arr))
            }
            None => Ok(col.clone()),
        }
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
