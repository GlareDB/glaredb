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
        Ok(Self::ProtoType { idx: self.idx as u32 })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            idx: proto.idx as usize,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::buffer::addressable::AddressableStorage;
    use crate::arrays::buffer::physical_type::{PhysicalDictionary, PhysicalI32};
    use crate::arrays::buffer::{Int32BufferBuilder, StringBufferBuilder};
    use crate::arrays::datatype::DataType;
    use crate::arrays::executor::scalar::unary::UnaryExecutor;

    #[test]
    fn eval_simple() {
        let mut batch = Batch::from_arrays(
            [
                Array::new_with_buffer(DataType::Int32, Int32BufferBuilder::from_iter([4, 5, 6]).unwrap()),
                Array::new_with_buffer(DataType::Utf8, StringBufferBuilder::from_iter(["a", "b", "c"]).unwrap()),
            ],
            true,
        )
        .unwrap();

        let expr = PhysicalColumnExpr { idx: 1 };
        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 3).unwrap();

        expr.eval(
            &mut batch,
            &mut ExpressionState::empty(),
            FlatSelection::linear(3),
            &mut out,
        )
        .unwrap();

        // Original array and output array should have both been made managed.
        assert!(batch.get_array(1).unwrap().data().is_managed());
        assert!(out.data().is_managed());

        // First array should have remained unchanged though.
        assert!(batch.get_array(0).unwrap().data().is_owned());

        let out_view = out.data().try_as_string_view_storage().unwrap();

        assert_eq!("a", out_view.get(0).unwrap());
        assert_eq!("b", out_view.get(1).unwrap());
        assert_eq!("c", out_view.get(2).unwrap());
    }

    #[test]
    fn eval_with_selection() {
        let mut batch = Batch::from_arrays(
            [Array::new_with_buffer(
                DataType::Int32,
                Int32BufferBuilder::from_iter([4, 5, 6]).unwrap(),
            )],
            true,
        )
        .unwrap();

        let expr = PhysicalColumnExpr { idx: 0 };
        let mut out = Array::new(&NopBufferManager, DataType::Int32, 2).unwrap();

        expr.eval(
            &mut batch,
            &mut ExpressionState::empty(),
            FlatSelection::selection(&[2, 0]),
            &mut out,
        )
        .unwrap();

        let managed_slice = out
            .flat_view()
            .unwrap()
            .array_buffer
            .try_as_slice::<PhysicalI32>()
            .unwrap();

        // Should be the same as in the batch.
        assert_eq!(&[4, 5, 6], managed_slice);

        // But with a selection stored in the array.
        let dict_slice = out.data.try_as_slice::<PhysicalDictionary>().unwrap();
        assert_eq!(&[2, 0], dict_slice);

        let mut out_buf = [None, None];
        UnaryExecutor::for_each_flat::<PhysicalI32, _>(out.flat_view().unwrap(), 0..2, |idx, v| {
            out_buf[idx] = v.cloned()
        })
        .unwrap();

        assert_eq!([Some(6), Some(4)], out_buf);
    }
}
