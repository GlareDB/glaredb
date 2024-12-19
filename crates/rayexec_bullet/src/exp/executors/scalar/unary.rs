use rayexec_error::Result;

use super::OutputBuffer;
use crate::exp::array::Array;
use crate::exp::buffer::addressable::AddressableStorage;
use crate::exp::buffer::physical_type::{MutablePhysicalStorage, PhysicalStorage};
use crate::exp::buffer::ArrayBuffer;
use crate::exp::validity::Validity;
use crate::selection::SelectionVector;

#[derive(Debug, Clone)]
pub struct UnaryExecutor;

impl UnaryExecutor {
    pub fn execute<'b, S, O, Op>(
        array: &Array,
        selection: &SelectionVector,
        out: &mut ArrayBuffer,
        out_validity: &mut Validity,
        mut op: Op,
    ) -> Result<()>
    where
        S: PhysicalStorage,
        O: MutablePhysicalStorage,
        for<'a> Op: FnMut(&S::StorageType, OutputBuffer<O::MutableStorage<'a>>),
    {
        let input = S::get_storage(array.buffer())?;
        let mut output = O::get_storage_mut(out)?;

        let validity = array.validity();

        if validity.all_valid() {
            for (output_idx, input_idx) in selection.iter_locations().enumerate() {
                op(
                    input.get(input_idx).unwrap(),
                    OutputBuffer {
                        idx: output_idx,
                        buffer: &mut output,
                    },
                );
            }
        } else {
            for (output_idx, input_idx) in selection.iter_locations().enumerate() {
                if validity.is_valid(input_idx) {
                    op(
                        input.get(input_idx).unwrap(),
                        OutputBuffer {
                            idx: output_idx,
                            buffer: &mut output,
                        },
                    );
                } else {
                    out_validity.set_invalid(output_idx)
                }
            }
        }
        Ok(())
    }
}
