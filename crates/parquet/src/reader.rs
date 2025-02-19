use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;

use rayexec_error::Result;
use rayexec_execution::arrays::batch::Batch;
use rayexec_execution::execution::operators::source::operation::{PollPull, Projections};
use rayexec_io::exp::{AsyncReadStream, FileSource};

use crate::file::metadata::ParquetMetaData;

#[derive(Debug)]
pub struct RowGroupsReader {
    metadata: Arc<ParquetMetaData>,
    /// File source we're reading from.
    source: Box<dyn FileSource>,
    /// Current row group we're reading from, or None if we have yet to begin
    /// reading.
    current_group: Option<usize>,
    /// Next row groups to read from.
    row_groups: VecDeque<usize>,
    /// Current fetch state.
    fetch_state: FetchState,
    /// Projected columns.
    projections: Projections,
}

#[derive(Debug)]
enum FetchState {
    /// We need to fetch data for a column.
    NeedsFetch {
        /// Column we need to fetch data for.
        ///
        /// Relative to the output projection.
        column_idx: usize,
    },
    /// We're currently fetching a column.
    Fetching {
        /// Column we're reading for.
        ///
        /// This index is relative to the output projection.
        column_idx: usize,
        /// Amount written to the columns buffer.
        amount_written: usize,
        /// Stream used for pulling data from the source.
        stream: Pin<Box<dyn AsyncReadStream>>,
    },
    /// All column chunks for this row group have been fetched, we can continue
    /// scanning.
    Fetched,
}

impl RowGroupsReader {
    pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        // Get or load the row group we're working on.
        let current = match self.current_group {
            Some(current) => current,
            None => {
                // Need to load in the next group.
                let next = match self.row_groups.pop_front() {
                    Some(next) => next,
                    None => {
                        // No more row groups to read, we're done.
                        output.set_num_rows(0)?;
                        return Ok(PollPull::Exhausted);
                    }
                };

                unimplemented!()
            }
        };

        // Fetch data if needed.
        loop {
            match &mut self.fetch_state {
                FetchState::NeedsFetch { column_idx } => {
                    // Init fetch stream.
                    let col = self
                        .metadata
                        .row_group(current)
                        .column(self.projections.column_indices[*column_idx]);
                    let (start, len) = col.byte_range();
                    let stream = self.source.read_range(start as usize, len as usize);

                    self.fetch_state = FetchState::Fetching {
                        column_idx: *column_idx,
                        amount_written: 0,
                        stream,
                    }
                }
                FetchState::Fetching {
                    column_idx,
                    amount_written,
                    stream,
                } => {
                    //
                    unimplemented!()
                }
                FetchState::Fetched => {
                    // Continue on to scanning.
                    break;
                }
            }
        }

        unimplemented!()
    }
}
