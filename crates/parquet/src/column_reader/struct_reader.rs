use rayexec_execution::arrays::array::buffer_manager::BufferManager;

use super::ColumnReadState;

#[derive(Debug)]
pub struct StructReader<B: BufferManager> {
    pub children: Vec<ColumnReadState<B>>,
}
