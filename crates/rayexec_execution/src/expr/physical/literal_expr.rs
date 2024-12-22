use std::borrow::Cow;
use std::fmt;

use rayexec_bullet::array::ArrayOld;
use rayexec_bullet::batch::BatchOld;
use rayexec_bullet::scalar::OwnedScalarValue;
use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use super::evaluator::ExpressionState;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::buffer_manager::NopBufferManager;
use crate::arrays::flat_array::FlatSelection;
use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLiteralExpr {
    pub literal: OwnedScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn eval2<'a>(&self, batch: &'a BatchOld) -> Result<Cow<'a, ArrayOld>> {
        let arr = self.literal.as_array(batch.num_rows())?;
        Ok(Cow::Owned(arr))
    }

    pub(crate) fn eval(
        &self,
        _: &mut Batch,
        _: &mut ExpressionState,
        sel: FlatSelection,
        output: &mut Array,
    ) -> Result<()> {
        output.set_value(&self.literal, 0)?;
        let const_sel = FlatSelection::constant(sel.len(), 0);

        output.select(&NopBufferManager, const_sel)?;

        Ok(())
    }
}

impl fmt::Display for PhysicalLiteralExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.literal)
    }
}

impl DatabaseProtoConv for PhysicalLiteralExpr {
    type ProtoType = rayexec_proto::generated::physical_expr::PhysicalLiteralExpr;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            literal: Some(self.literal.to_proto()?),
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            literal: ProtoConv::from_proto(proto.literal.required("literal")?)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use rayexec_bullet::scalar::ScalarValue;

    use super::*;
    use crate::arrays::buffer::physical_type::PhysicalI32;
    use crate::arrays::buffer::Int32Builder;
    use crate::arrays::datatype::DataType;
    use crate::arrays::executor::scalar::unary::UnaryExecutor;

    #[test]
    fn eval_simple() {
        let mut batch = Batch::from_arrays(
            [Array::new_with_buffer(
                DataType::Int32,
                Int32Builder::from_iter([4, 5, 6]).unwrap(),
            )],
            true,
        )
        .unwrap();

        let expr = PhysicalLiteralExpr {
            literal: ScalarValue::Int32(48),
        };

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 4096).unwrap();
        expr.eval(
            &mut batch,
            &mut ExpressionState::empty(),
            FlatSelection::linear(3),
            &mut out,
        )
        .unwrap();

        let mut out_buf = [None, None, None];
        UnaryExecutor::for_each_flat::<PhysicalI32, _>(out.flat_view().unwrap(), 0..3, |idx, v| {
            out_buf[idx] = v.cloned();
        })
        .unwrap();

        assert_eq!([Some(48), Some(48), Some(48)], out_buf);
    }

    #[test]
    fn eval_simple_null() {
        let mut batch = Batch::from_arrays(
            [Array::new_with_buffer(
                DataType::Int32,
                Int32Builder::from_iter([4, 5, 6]).unwrap(),
            )],
            true,
        )
        .unwrap();

        let expr = PhysicalLiteralExpr {
            literal: ScalarValue::Null,
        };

        let mut out = Array::new(&NopBufferManager, DataType::Int32, 4096).unwrap();
        expr.eval(
            &mut batch,
            &mut ExpressionState::empty(),
            FlatSelection::linear(3),
            &mut out,
        )
        .unwrap();

        let mut out_buf = [None, None, None];
        UnaryExecutor::for_each_flat::<PhysicalI32, _>(out.flat_view().unwrap(), 0..3, |idx, v| {
            out_buf[idx] = v.cloned();
        })
        .unwrap();

        assert_eq!([None, None, None], out_buf);
    }
}
