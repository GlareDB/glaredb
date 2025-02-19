use buffer::ReadBuffer;
use rayexec_execution::arrays::array::buffer_manager::BufferManager;

pub mod buffer;
pub mod primitive_reader;
pub mod struct_reader;

#[derive(Debug)]
pub struct ColumnReadState<B: BufferManager> {
    /// Buffer holding decompressed column data.
    decompressed: ReadBuffer<B>,
    /// Buffer holding compressed column data.
    ///
    /// If the parquet source does not use compression, this will be empty.
    compressed: ReadBuffer<B>,
    /// Remaining number of rows in the current page.
    remaining_page_rows: usize,
    /// Remaining number of rows in the current group.
    remaining_group_rows: usize,
}
