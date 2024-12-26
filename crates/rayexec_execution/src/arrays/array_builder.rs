use iterutil::exact_size::IntoExactSizeIterator;
use rayexec_error::Result;

use super::array::Array;
use super::buffer::physical_type::PhysicalStorage;
use super::buffer_manager::BufferManager;
use super::datatype::DataType;

#[derive(Debug)]
pub struct Int8Builder<'a, B: BufferManager> {
    pub manager: &'a B,
}
