use std::fmt;

use rayexec_error::Result;

use super::ExpressionState;
use crate::arrays::array::selection::Selection;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::DataType;
use crate::arrays::scalar::ScalarValue;
use crate::buffer::buffer_manager::NopBufferManager;

#[derive(Debug, Clone, PartialEq)]
pub struct PhysicalLiteralExpr {
    pub literal: ScalarValue,
}

impl PhysicalLiteralExpr {
    pub fn new(literal: impl Into<ScalarValue>) -> Self {
        PhysicalLiteralExpr {
            literal: literal.into(),
        }
    }

    pub(crate) fn create_state(&self, _batch_size: usize) -> Result<ExpressionState> {
        Ok(ExpressionState::empty())
    }

    pub fn datatype(&self) -> DataType {
        self.literal.datatype()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::datatype::DataType;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

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
            Selection::linear(0, 4),
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
            Selection::slice(&[2, 3]),
            &mut out,
        )
        .unwrap();

        let expected = Array::try_from_iter(["catdog", "catdog"]).unwrap();
        assert_arrays_eq(&expected, &out);
    }
}
