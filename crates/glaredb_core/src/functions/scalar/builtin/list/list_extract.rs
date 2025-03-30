use glaredb_error::{DbError, Result, not_implemented};

use crate::arrays::array::Array;
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutableScalarStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI8,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI128,
    PhysicalInterval,
    PhysicalType,
    PhysicalU8,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU128,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId};
use crate::expr::Expression;
use crate::functions::Signature;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::function_set::ScalarFunctionSet;
use crate::functions::scalar::{BindState, RawScalarFunction, ScalarFunction};
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;
use crate::util::iter::IntoExactSizeIterator;

pub const FUNCTION_SET_LIST_EXTRACT: ScalarFunctionSet = ScalarFunctionSet {
    name: "list_extract",
    aliases: &[],
    doc: &[&Documentation {
        category: Category::List,
        description: "Extract an item from the list. Used 1-based indexing.",
        arguments: &["list", "index"],
        example: Some(Example {
            example: "list_extract([4,5,6], 2)",
            output: "5",
        }),
    }],
    functions: &[RawScalarFunction::new(
        &Signature::new(
            &[DataTypeId::List(&DataTypeId::Any), DataTypeId::Int64],
            DataTypeId::Any,
        ),
        &ListExtract,
    )],
};

#[derive(Debug)]
pub struct ListExtractState {
    index: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListExtract;

impl ScalarFunction for ListExtract {
    type State = ListExtractState;

    fn bind(&self, inputs: Vec<Expression>) -> Result<BindState<Self::State>> {
        let index = ConstFold::rewrite(inputs[1].clone())?
            .try_into_scalar()?
            .try_as_i64()?;

        if index <= 0 {
            return Err(DbError::new("Index cannot be less than 1"));
        }
        // Adjust from 1-based indexing.
        let index = (index - 1) as usize;

        let inner_datatype = match inputs[0].datatype()? {
            DataType::List(meta) => meta.datatype.as_ref().clone(),
            other => {
                return Err(DbError::new(format!(
                    "Cannot index into non-list type, got {other}",
                )));
            }
        };

        Ok(BindState {
            state: ListExtractState { index },
            return_type: inner_datatype,
            inputs,
        })
    }

    fn execute(state: &Self::State, input: &Batch, output: &mut Array) -> Result<()> {
        let sel = input.selection();
        let input = &input.arrays()[0];
        list_extract(input, sel, output, state.index)
    }
}

/// Extract an element from each list within a list array.
///
/// If the element index falls outside the bounds of a list, the result for that
/// row will be NULL.
pub fn list_extract(
    array: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
    element_idx: usize,
) -> Result<()> {
    match output.datatype().physical_type() {
        PhysicalType::UntypedNull => {
            extract_inner::<PhysicalUntypedNull>(array, sel, output, element_idx)
        }
        PhysicalType::Boolean => extract_inner::<PhysicalBool>(array, sel, output, element_idx),
        PhysicalType::Int8 => extract_inner::<PhysicalI8>(array, sel, output, element_idx),
        PhysicalType::Int16 => extract_inner::<PhysicalI16>(array, sel, output, element_idx),
        PhysicalType::Int32 => extract_inner::<PhysicalI32>(array, sel, output, element_idx),
        PhysicalType::Int64 => extract_inner::<PhysicalI64>(array, sel, output, element_idx),
        PhysicalType::Int128 => extract_inner::<PhysicalI128>(array, sel, output, element_idx),
        PhysicalType::UInt8 => extract_inner::<PhysicalU8>(array, sel, output, element_idx),
        PhysicalType::UInt16 => extract_inner::<PhysicalU16>(array, sel, output, element_idx),
        PhysicalType::UInt32 => extract_inner::<PhysicalU32>(array, sel, output, element_idx),
        PhysicalType::UInt64 => extract_inner::<PhysicalU64>(array, sel, output, element_idx),
        PhysicalType::UInt128 => extract_inner::<PhysicalU128>(array, sel, output, element_idx),
        PhysicalType::Float16 => extract_inner::<PhysicalF16>(array, sel, output, element_idx),
        PhysicalType::Float32 => extract_inner::<PhysicalF32>(array, sel, output, element_idx),
        PhysicalType::Float64 => extract_inner::<PhysicalF64>(array, sel, output, element_idx),
        PhysicalType::Interval => {
            extract_inner::<PhysicalInterval>(array, sel, output, element_idx)
        }
        PhysicalType::Utf8 => extract_inner::<PhysicalUtf8>(array, sel, output, element_idx),
        PhysicalType::Binary => extract_inner::<PhysicalBinary>(array, sel, output, element_idx),
        other => not_implemented!("List extract for datatype {other}"),
    }
}

fn extract_inner<S>(
    array: &Array,
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
    element_idx: usize,
) -> Result<()>
where
    S: MutableScalarStorage,
{
    let flat = array.flatten()?;

    let list_buf = flat.array_buffer.get_list_buffer()?;
    let metas = list_buf.metadata.as_slice();

    let child_buf = S::get_addressable(&list_buf.child_buffer)?;
    let child_validity = &list_buf.child_validity;

    let mut out_buffer = S::get_addressable_mut(&mut output.data)?;
    let out_validity = &mut output.validity;

    for (output_idx, input_idx) in sel.into_iter().enumerate() {
        let sel_idx = flat.selection.get(input_idx).unwrap();

        if flat.validity.is_valid(input_idx) {
            let meta = metas.get(sel_idx).unwrap();
            if element_idx >= meta.len as usize {
                // Indexing outside of the list. User is allowed to do that, set
                // the value to null.
                out_validity.set_invalid(output_idx);
                continue;
            }

            let offset = meta.offset as usize + element_idx;
            if !child_validity.is_valid(offset) {
                // Element inside list is null.
                out_validity.set_invalid(output_idx);
                continue;
            }

            let val = child_buf.get(offset).unwrap();
            out_buffer.put(output_idx, val);
        } else {
            out_validity.set_invalid(output_idx);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::arrays::compute::make_list::make_list_from_values;
    use crate::arrays::datatype::ListTypeMeta;
    use crate::buffer::buffer_manager::NopBufferManager;
    use crate::testutil::arrays::assert_arrays_eq;
    use crate::util::iter::TryFromExactSizeIterator;

    #[test]
    fn list_extract_primitive() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut lists = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            3,
        )
        .unwrap();

        make_list_from_values(&[a, b], 0..3, &mut lists).unwrap();

        let mut second_elements = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        list_extract(&lists, 0..3, &mut second_elements, 1).unwrap();

        let expected = Array::try_from_iter([4, 5, 6]).unwrap();
        assert_arrays_eq(&expected, &second_elements);
    }

    #[test]
    fn list_extract_out_of_bounds() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut lists = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            3,
        )
        .unwrap();

        make_list_from_values(&[a, b], 0..3, &mut lists).unwrap();

        let mut extracted_elements = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        list_extract(&lists, 0..3, &mut extracted_elements, 2).unwrap();

        let expected = Array::try_from_iter([None as Option<i32>, None, None]).unwrap();
        assert_arrays_eq(&expected, &extracted_elements);
    }

    #[test]
    fn list_extract_child_invalid() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([Some(4), None, Some(6)]).unwrap();

        let mut lists = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            3,
        )
        .unwrap();

        make_list_from_values(&[a, b], 0..3, &mut lists).unwrap();

        let mut second_elements = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        list_extract(&lists, 0..3, &mut second_elements, 1).unwrap();

        let expected = Array::try_from_iter([Some(4), None, Some(6)]).unwrap();
        assert_arrays_eq(&expected, &second_elements);

        // Elements as index 0 should still be all non-null.
        let mut first_elements = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        list_extract(&lists, 0..3, &mut first_elements, 0).unwrap();

        let expected = Array::try_from_iter([1, 2, 3]).unwrap();
        assert_arrays_eq(&expected, &first_elements);
    }

    #[test]
    fn list_extract_parent_invalid() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();

        let mut lists = Array::new(
            &NopBufferManager,
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            3,
        )
        .unwrap();

        make_list_from_values(&[a, b], 0..3, &mut lists).unwrap();
        lists.validity.set_invalid(1); // [2, 5] => NULL

        let mut second_elements = Array::new(&NopBufferManager, DataType::Int32, 3).unwrap();
        list_extract(&lists, 0..3, &mut second_elements, 1).unwrap();

        let expected = Array::try_from_iter([Some(4), None, Some(6)]).unwrap();
        assert_arrays_eq(&expected, &second_elements);
    }
}
