use crate::arrays::array::buffer_manager::NopBufferManager;
use crate::arrays::batch::Batch;
use crate::arrays::row::block::Block;
use crate::arrays::row::row_collection::RowCollection;
use crate::arrays::row::row_layout::RowLayout;

#[derive(Debug)]
pub struct TestRowBlock {
    pub layout: RowLayout,
    pub rows: Block<NopBufferManager>,
    pub heap: Block<NopBufferManager>,
}

impl TestRowBlock {
    /// Create a TestBlock from a batch.
    #[track_caller]
    pub fn from_batch(batch: &Batch) -> Self {
        let layout = RowLayout::new(batch.arrays.iter().map(|a| a.datatype.clone()));
        let mut collection = RowCollection::new(layout.clone(), batch.num_rows());

        let mut state = collection.init_append();
        collection.append_batch(&mut state, batch).unwrap();

        let (mut row_blocks, mut heap_blocks) = collection.blocks_mut().take_blocks();
        assert_eq!(1, row_blocks.len());
        assert!(heap_blocks.len() == 0 || heap_blocks.len() == 1);

        let rows = row_blocks.pop().unwrap();
        let heap = match heap_blocks.pop() {
            Some(block) => block,
            None => Block::try_new(&NopBufferManager, 0).unwrap(),
        };

        TestRowBlock { layout, rows, heap }
    }
}
