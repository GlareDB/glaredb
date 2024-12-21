use std::borrow::Cow;
use std::fmt;

use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::batch::BatchOld;
use rayexec_error::{RayexecError, Result};

use super::evaluator::ExpressionState;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::buffer_manager::NopBufferManager;
use crate::arrays::flat_array::FlatSelection;
use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone)]
pub struct PhysicalColumnExpr {
    pub idx: usize,
}

impl PhysicalColumnExpr {
    pub fn eval2<'a>(&self, batch: &'a BatchOld) -> Result<Cow<'a, ArrayOld>> {
        let col = batch.column(self.idx).ok_or_else(|| {
            RayexecError::new(format!(
                "Tried to get column at index {} in a batch with {} columns",
                self.idx,
                batch.columns().len()
            ))
        })?;

        Ok(Cow::Borrowed(col))
    }

    pub(crate) fn eval(
        &self,
        input: &mut Batch,
        _: &mut ExpressionState,
        sel: FlatSelection,
        output: &mut Array,
    ) -> Result<()> {
        let array = input.get_array_mut(self.idx)?;
        output.make_managed_from(&NopBufferManager, array)?;

        if !sel.is_linear() || sel.len() != input.num_rows() {
            output.select(&NopBufferManager, sel.iter())?;
        }

        Ok(())
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
