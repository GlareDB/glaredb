use glaredb_core::arrays::array::Array;
use glaredb_error::Result;

use super::Definitions;
use crate::column::read_buffer::ReadCursor;

#[derive(Debug)]
pub struct DeltaLengthByteArrayDecoder {}

impl DeltaLengthByteArrayDecoder {
    pub fn try_new(cursor: ReadCursor) -> Result<Self> {
        unimplemented!()
    }

    pub fn read(
        &mut self,
        definitions: Definitions,
        output: &mut Array,
        offset: usize,
        count: usize,
    ) -> Result<()> {
        unimplemented!()
    }
}
