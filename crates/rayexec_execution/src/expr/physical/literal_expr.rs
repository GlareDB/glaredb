use std::borrow::Cow;
use std::fmt;

use rayexec_error::{OptionExt, Result};
use rayexec_proto::ProtoConv;

use super::evaluator::ExpressionState;
use crate::arrays::array::exp::Array;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array2;
use crate::arrays::batch::Batch2;
use crate::arrays::batch_exp::Batch;
use crate::arrays::buffer::buffer_manager::NopBufferManager;
use crate::arrays::scalar::OwnedScalarValue;
use crate::database::DatabaseContext;
use crate::proto::DatabaseProtoConv;

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLiteralExpr {
    pub literal: OwnedScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn eval2<'a>(&self, batch: &'a Batch2) -> Result<Cow<'a, Array2>> {
        let arr = self.literal.as_array(batch.num_rows())?;
        Ok(Cow::Owned(arr))
    }

    pub(crate) fn eval(
        &self,
        _: &mut Batch,
        _: &mut ExpressionState,
        sel: Selection,
        output: &mut Array,
    ) -> Result<()> {
        output.set_value(0, &self.literal)?;

        // TODO: Need to be able to provide "constant" selection here.
        output.select(&NopBufferManager, std::iter::repeat(0).take(sel.len()))?;

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
    use iterutil::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::arrays::testutil::assert_arrays_eq;

    #[test]
    fn literal_eval() {
        let mut input = Batch::empty_with_num_rows(4);

        let expr = PhysicalLiteralExpr {
            literal: "catdog".into(),
        };

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 4).unwrap();
        expr.eval(
            &mut input,
            &mut ExpressionState::empty(),
            Selection::linear(4),
            &mut out,
        )
        .unwrap();

        let expected = Array::try_from_iter(["catdog", "catdog", "catdog", "catdog"]).unwrap();
        assert_arrays_eq(&expected, &out);
    }

    #[test]
    fn literal_eval_with_selection() {
        let mut input = Batch::empty_with_num_rows(4);

        let expr = PhysicalLiteralExpr {
            literal: "catdog".into(),
        };

        let mut out = Array::new(&NopBufferManager, DataType::Utf8, 4).unwrap();
        expr.eval(
            &mut input,
            &mut ExpressionState::empty(),
            Selection::selection(&[2, 3]),
            &mut out,
        )
        .unwrap();

        let expected = Array::try_from_iter(["catdog", "catdog"]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
