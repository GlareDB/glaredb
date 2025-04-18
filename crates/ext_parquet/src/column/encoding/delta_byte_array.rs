use glaredb_core::arrays::array::Array;
use glaredb_core::arrays::array::physical_type::{MutableScalarStorage, PhysicalBinary};
use glaredb_error::Result;

use super::Definitions;
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
pub struct DeltaByteArrayDecoder {
    /// If we should verify that the bytes we read is valid utf8.
    verify_utf8: bool,
}

impl DeltaByteArrayDecoder {
    pub fn try_new(cursor: ReadCursor, num_values: usize, verify_utf8: bool) -> Result<Self> {
        unimplemented!()
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        let (data, validity) = output.data_and_validity_mut();
        let mut data = PhysicalBinary::get_addressable_mut(data)?;

        match definitions {
            Definitions::HasDefinitions { levels, max } => Ok(()),
            Definitions::NoDefinitions => Ok(()),
        }
    }
}
