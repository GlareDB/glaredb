use super::block::Block;

/// State for scanning a row block conforming to some row layout.
#[derive(Debug)]
pub struct BlockScanState {
    /// Pointers to the start of each row to read from.
    pub row_pointers: Vec<*const u8>,
}

// SAFETY: The `Vec<*const u8>` is just a buffer for storing row pointers.
unsafe impl Send for BlockScanState {}
unsafe impl Sync for BlockScanState {}

impl BlockScanState {
    pub const fn empty() -> Self {
        BlockScanState {
            row_pointers: Vec::new(),
        }
    }

    /// Clear all pointers from this state.
    pub fn clear(&mut self) {
        self.row_pointers.clear();
    }

    pub fn row_pointers_iter(&self) -> impl Iterator<Item = *const u8> + Clone + '_ {
        self.row_pointers.iter().copied()
    }

    /// Prepares this state to begin scanning the provided block.
    ///
    /// `selection` provides a row selection for which rows to scan from the
    /// block.
    ///
    /// This will clear out existing pointers if `clear` is true.
    ///
    /// # Safety
    ///
    /// - All indices must be in bounds of the block when multiplied by
    ///   `row_width`.
    ///
    /// # Correctness
    ///
    /// The block must have been allocated for rows of `row_width` size. This is
    /// only valid to use for blocks that have created using either `RowLayout`
    /// or `SortLayout`. It's never valid to attempt to scan a heap block (as
    /// they have no fixed layout).
    pub(crate) unsafe fn prepare_block_scan(
        &mut self,
        block: &Block,
        row_width: usize,
        selection: impl IntoIterator<Item = usize>,
        clear: bool,
    ) { unsafe {
        if clear {
            self.row_pointers.clear();
        }
        let block_ptr = block.as_ptr();

        for sel_idx in selection {
            debug_assert!(sel_idx < block.num_rows(row_width));
            let ptr = block_ptr.byte_add(row_width * sel_idx);
            debug_assert!(block.data.raw.contains_addr(ptr.addr()));

            self.row_pointers.push(ptr);
        }
    }}
}
