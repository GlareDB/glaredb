use rayexec_error::{not_implemented, RayexecError, Result};
use stdutil::iter::IntoExactSizeIterator;

use crate::arrays::array::array_buffer::{ListItemMetadata, SecondaryBuffer};
use crate::arrays::array::physical_type::{
    Addressable,
    AddressableMut,
    MutablePhysicalStorage,
    PhysicalBinary,
    PhysicalBool,
    PhysicalF16,
    PhysicalF32,
    PhysicalF64,
    PhysicalI128,
    PhysicalI16,
    PhysicalI32,
    PhysicalI64,
    PhysicalI8,
    PhysicalList,
    PhysicalType,
    PhysicalU128,
    PhysicalU16,
    PhysicalU32,
    PhysicalU64,
    PhysicalU8,
    PhysicalUntypedNull,
    PhysicalUtf8,
};
use crate::arrays::array::validity::Validity;
use crate::arrays::array::Array;
use crate::arrays::batch::Batch;
use crate::arrays::datatype::{DataType, DataTypeId, ListTypeMeta};
use crate::expr::Expression;
use crate::functions::documentation::{Category, Documentation, Example};
use crate::functions::scalar::{PlannedScalarFunction, ScalarFunction, ScalarFunctionImpl};
use crate::functions::{FunctionInfo, Signature};
use crate::logical::binder::table_list::TableList;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListValues;

impl FunctionInfo for ListValues {
    fn name(&self) -> &'static str {
        "list_values"
    }

    fn signatures(&self) -> &[Signature] {
        &[Signature {
            positional_args: &[],
            variadic_arg: Some(DataTypeId::Any),
            return_type: DataTypeId::List,
            doc: Some(&Documentation {
                category: Category::List,
                description: "Create a list fromt the given values.",
                arguments: &["var_arg"],
                example: Some(Example {
                    example: "list_values('cat', 'dog', 'mouse')",
                    output: "[cat, dog, mouse]",
                }),
            }),
        }]
    }
}

impl ScalarFunction for ListValues {
    fn plan(
        &self,
        table_list: &TableList,
        inputs: Vec<Expression>,
    ) -> Result<PlannedScalarFunction> {
        let first = match inputs.first() {
            Some(expr) => expr.datatype(table_list)?,
            None => {
                let return_type = DataType::List(ListTypeMeta {
                    datatype: Box::new(DataType::Null),
                });
                return Ok(PlannedScalarFunction {
                    function: Box::new(*self),
                    return_type: return_type.clone(),
                    inputs,
                    function_impl: Box::new(ListValuesImpl),
                });
            }
        };

        for input in &inputs {
            let dt = input.datatype(table_list)?;
            // TODO: We can add casts here.
            if dt != first {
                return Err(RayexecError::new(format!(
                    "Not all inputs are the same type, got {dt}, expected {first}"
                )));
            }
        }

        let return_type = DataType::List(ListTypeMeta {
            datatype: Box::new(first.clone()),
        });

        Ok(PlannedScalarFunction {
            function: Box::new(*self),
            return_type: return_type.clone(),
            inputs,
            function_impl: Box::new(ListValuesImpl),
        })
    }
}

#[derive(Debug, Clone)]
pub struct ListValuesImpl;

impl ScalarFunctionImpl for ListValuesImpl {
    fn execute(&self, input: &Batch, output: &mut Array) -> Result<()> {
        list_values(input.arrays(), input.selection(), output)
    }
}

pub fn list_values(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
) -> Result<()> {
    let inner_type = match output.datatype() {
        DataType::List(m) => m.datatype.physical_type(),
        other => {
            return Err(RayexecError::new(format!(
                "Expected output to be list datatype, got {other}",
            )))
        }
    };

    match inner_type {
        PhysicalType::UntypedNull => list_values_inner::<PhysicalUntypedNull>(inputs, sel, output),
        PhysicalType::Boolean => list_values_inner::<PhysicalBool>(inputs, sel, output),
        PhysicalType::Int8 => list_values_inner::<PhysicalI8>(inputs, sel, output),
        PhysicalType::Int16 => list_values_inner::<PhysicalI16>(inputs, sel, output),
        PhysicalType::Int32 => list_values_inner::<PhysicalI32>(inputs, sel, output),
        PhysicalType::Int64 => list_values_inner::<PhysicalI64>(inputs, sel, output),
        PhysicalType::Int128 => list_values_inner::<PhysicalI128>(inputs, sel, output),
        PhysicalType::UInt8 => list_values_inner::<PhysicalU8>(inputs, sel, output),
        PhysicalType::UInt16 => list_values_inner::<PhysicalU16>(inputs, sel, output),
        PhysicalType::UInt32 => list_values_inner::<PhysicalU32>(inputs, sel, output),
        PhysicalType::UInt64 => list_values_inner::<PhysicalU64>(inputs, sel, output),
        PhysicalType::UInt128 => list_values_inner::<PhysicalU128>(inputs, sel, output),
        PhysicalType::Float16 => list_values_inner::<PhysicalF16>(inputs, sel, output),
        PhysicalType::Float32 => list_values_inner::<PhysicalF32>(inputs, sel, output),
        PhysicalType::Float64 => list_values_inner::<PhysicalF64>(inputs, sel, output),
        PhysicalType::Utf8 => list_values_inner::<PhysicalUtf8>(inputs, sel, output),
        PhysicalType::Binary => list_values_inner::<PhysicalBinary>(inputs, sel, output),
        other => not_implemented!("list values for physical type {other}"),
    }
}

/// Helper for constructing the list values and writing them to `output`.
///
/// `S` should be the inner type.
fn list_values_inner<S: MutablePhysicalStorage>(
    inputs: &[Array],
    sel: impl IntoExactSizeIterator<Item = usize>,
    output: &mut Array,
) -> Result<()> {
    // TODO: Dictionary

    let sel = sel.into_iter();
    let sel_len = sel.len();
    let capacity = sel.len() * inputs.len();

    let list_buf = match output.next_mut().data.try_as_mut()?.get_secondary_mut() {
        SecondaryBuffer::List(list) => list,
        _ => return Err(RayexecError::new("Expected list buffer")),
    };

    // Resize secondary buffer (and validity) to hold everything.
    //
    // TODO: Need to store buffer manager somewhere else.
    list_buf
        .child
        .next_mut()
        .data
        .try_as_mut()?
        .reserve_primary::<S>(capacity)?;

    // Replace validity with properly sized one.
    list_buf
        .child
        .put_validity(Validity::new_all_valid(capacity))?;

    // Update metadata on the list buffer itself. Note that this can be less
    // than the buffer's actual capacity. This only matters during writes to
    // know if we still have room to push to the child array.
    list_buf.entries = capacity;

    let child_next = list_buf.child.next_mut();
    let mut child_outputs = S::get_addressable_mut(child_next.data.try_as_mut()?)?;
    let child_validity = &mut child_next.validity;

    // TODO: Possibly avoid allocating here?
    let col_bufs = inputs
        .iter()
        .map(|arr| S::get_addressable(&arr.next().data))
        .collect::<Result<Vec<_>>>()?;

    // Write the list values from the input batch.
    let mut output_idx = 0;
    for row_idx in sel {
        for (col, validity) in col_bufs
            .iter()
            .zip(inputs.iter().map(|arr| &arr.next().validity))
        {
            if validity.is_valid(row_idx) {
                child_outputs.put(output_idx, col.get(row_idx).unwrap());
            } else {
                child_validity.set_invalid(output_idx);
            }

            output_idx += 1;
        }
    }
    std::mem::drop(child_outputs);

    // Now generate and set the metadatas.
    let mut out =
        PhysicalList::get_addressable_mut(output.next.as_mut().unwrap().data.try_as_mut()?)?;

    let len = inputs.len() as i32;
    for output_idx in 0..sel_len {
        // Note top-level not possible if we're provided a batch.
        out.put(
            output_idx,
            &ListItemMetadata {
                offset: (output_idx as i32) * len,
                len,
            },
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use stdutil::iter::TryFromExactSizeIterator;

    use super::*;
    use crate::arrays::array::buffer_manager::NopBufferManager;
    use crate::arrays::array::physical_type::PhysicalStorage;
    use crate::expr;

    #[test]
    fn list_values_primitive() {
        let a = Array::try_from_iter([1, 2, 3]).unwrap();
        let b = Array::try_from_iter([4, 5, 6]).unwrap();
        let batch = Batch::try_from_arrays([a, b]).unwrap();

        let mut table_list = TableList::empty();
        let table_ref = table_list
            .push_table(
                None,
                vec![DataType::Int32, DataType::Int32],
                vec!["a".to_string(), "b".to_string()],
            )
            .unwrap();

        let planned = ListValues
            .plan(
                &table_list,
                vec![expr::col_ref(table_ref, 0), expr::col_ref(table_ref, 1)],
            )
            .unwrap();

        let mut out = Array::try_new(
            &Arc::new(NopBufferManager),
            DataType::List(ListTypeMeta::new(DataType::Int32)),
            3,
        )
        .unwrap();
        planned.function_impl.execute(&batch, &mut out).unwrap();

        // TODO: Assert list equality.

        let expected_metas = &[
            ListItemMetadata { offset: 0, len: 2 },
            ListItemMetadata { offset: 2, len: 2 },
            ListItemMetadata { offset: 4, len: 2 },
        ];

        let s = PhysicalList::get_addressable(&out.next().data).unwrap();
        assert_eq!(expected_metas, s);
    }
}
