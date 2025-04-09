use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_core::arrays::batch::Batch;
use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::execution::operators::PollPull;
use glaredb_core::runtime::filesystem::AnyFile;
use glaredb_core::storage::projections::Projections;
use glaredb_error::Result;

use crate::column::column_reader::ColumnReader;
use crate::column::struct_reader::StructReader;
use crate::metadata::ParquetMetaData;

#[derive(Debug)]
pub struct Reader {
    /// Metadata for the file we're currently reading.
    metadata: Arc<ParquetMetaData>,
    /// File we're reading from.
    file: AnyFile,
    /// Queue of row groups to read from.
    row_groups: VecDeque<usize>,
    /// State of the current row group we're reading.
    state: RowGroupState,
    /// State for the fetching data from the file.
    fetch_state: FetchState,
    /// Column projections for this file.
    projections: Projections,
    /// Reader for the root of the schema.
    root: StructReader,
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
    /// We're currently seeking for a column.
    Seeking {
        /// Seek to the beginning of the column chunk.
        seek: io::SeekFrom,
        /// The column we're seeking for.
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
    },
    /// All column chunks for this row group have been fetched, we can continue
    /// scanning.
    Fetched,
}

#[derive(Debug)]
struct RowGroupState {
    /// Index of the current group we're scanning.
    ///
    /// The initial value for this doesn't matter as we'll immediately try to
    /// get the next row group from the queue since the number of rows remaining
    /// will be zero.
    current_group: usize,
    /// Remaining number of rows in the group.
    ///
    /// This gets set when we read in a new group, and continuously updated as
    /// we read values.
    ///
    /// Once this reaches zero, the next row group should be fetched.
    remaining_group_rows: usize,
}

impl Reader {
    /// Create a new reader that reads from the given file.
    ///
    /// `groups` indicates which row groups this reader will read. Groups are
    /// read in the order provided by the iterator.
    pub fn try_new(
        manager: &impl AsRawBufferManager,
        metadata: Arc<ParquetMetaData>,
        file: AnyFile,
        groups: impl IntoIterator<Item = usize>,
        projections: Projections,
    ) -> Result<Self> {
        let readers = projections
            .data_indices()
            .iter()
            .map(|&col_idx| {
                // TODO: I'll fix this later, we're just assuming a flat schema
                // right now.
                let col_descr = metadata.file_metadata.schema_descr.leaves[col_idx].clone();
                ColumnReader::try_new(manager, col_descr)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Reader {
            metadata,
            file,
            row_groups: groups.into_iter().collect(),
            state: RowGroupState {
                current_group: 0,
                remaining_group_rows: 0, // Will trigger getting the real first row group.
            },
            fetch_state: FetchState::NeedsFetch { column_idx: 0 }, // This doesn't matter here.
            projections,
            root: StructReader { readers },
        })
    }

    /// Scan rows into the output batch.
    ///
    /// This will fetch column chunks as needed.
    pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        if self.state.remaining_group_rows == 0 {
            // No more rows, get the next row group.
            let next = match self.row_groups.pop_front() {
                Some(group) => group,
                None => {
                    // No more row groups, we're done.
                    output.set_num_rows(0)?;
                    return Ok(PollPull::Exhausted);
                }
            };

            let group = &self.metadata.row_groups[next];
            self.state = RowGroupState {
                current_group: next,
                remaining_group_rows: group.num_rows as usize,
            };

            // Init fetch.
            self.fetch_state = FetchState::NeedsFetch { column_idx: 0 };
            // Continue on... The fetch state will trigger fetching of the
            // columns.
        }

        match self.poll_fetch(cx)? {
            Poll::Ready(_) => (), // Good to go, we have all column chunks.
            Poll::Pending => return Ok(PollPull::Pending), // Pending read, state updated to resume fetching of a column chunk.
        }
        debug_assert!(matches!(self.fetch_state, FetchState::Fetched));

        // Now we can scan. No async nonsense happens after this point.

        let cap = output.write_capacity()?;
        let count = usize::min(self.state.remaining_group_rows, cap);

        debug_assert_eq!(output.arrays().len(), self.root.readers.len());

        for (output, col_reader) in output.arrays_mut().iter_mut().zip(&mut self.root.readers) {
            col_reader.read(output, count)?;
            self.state.remaining_group_rows -= count;
        }

        output.set_num_rows(count)?;

        Ok(PollPull::HasMore)
    }

    /// Fetches data for all columns in the row group.
    ///
    /// This will update the page readers for each column to hold its new column
    /// chunk.
    ///
    /// Fetch happens async. The state of this read will track the current
    /// column chunk we're fetching, and will pick up where it left off on the
    /// next poll.
    ///
    /// This may be called even if all column chunks have been fetched. The
    /// internal state will just make this immediately return.
    fn poll_fetch(&mut self, cx: &mut Context) -> Result<Poll<()>> {
        let row_group = &self.metadata.row_groups[self.state.current_group];
        loop {
            match &mut self.fetch_state {
                FetchState::NeedsFetch { column_idx } => {
                    let real_idx = self.projections.data_indices()[*column_idx];
                    let col = &row_group.columns[real_idx];

                    let (start, len) = col.byte_range();

                    // Reset and resize this column's buffer.
                    let reader = &mut self.root.readers[*column_idx];
                    reader.page_reader.chunk_offset = 0;
                    reader.page_reader.chunk.reserve_for_size(len as usize)?;

                    // Begin seeking to the start of this column chunk.
                    self.fetch_state = FetchState::Seeking {
                        seek: io::SeekFrom::Start(start),
                        column_idx: *column_idx,
                    };
                    // Continue...
                }
                FetchState::Seeking { seek, column_idx } => {
                    // Seek in the file.
                    match self.file.call_poll_seek(cx, *seek) {
                        Poll::Ready(Ok(())) => {
                            // Begin fetching.
                            self.fetch_state = FetchState::Fetching {
                                column_idx: *column_idx,
                                amount_written: 0,
                            }
                            // Continue...
                        }
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(Poll::Pending),
                    }
                }
                FetchState::Fetching {
                    column_idx,
                    amount_written,
                } => {
                    // Read into this column's buffer. Buffer should be the
                    // exact size.
                    let buf = self.root.readers[*column_idx]
                        .page_reader
                        .chunk
                        .as_slice_mut();

                    let read_buf = &mut buf[*amount_written..];
                    match self.file.call_poll_read(cx, read_buf) {
                        Poll::Ready(Ok(n)) => {
                            *amount_written += n;

                            if *amount_written == buf.len() {
                                // We've read everyting for this column, move to
                                // the next column, or indicate everything's
                                // been fetched if this is the last column.
                                if *column_idx == self.root.readers.len() - 1 {
                                    // We're done fetching.
                                    self.fetch_state = FetchState::Fetched;
                                    return Ok(Poll::Ready(()));
                                }

                                // Move to next column.
                                self.fetch_state = FetchState::NeedsFetch {
                                    column_idx: *column_idx + 1,
                                };
                                // Continue...
                            }
                        }
                        Poll::Ready(Err(e)) => return Err(e),
                        Poll::Pending => return Ok(Poll::Pending),
                    }
                }
                FetchState::Fetched => {
                    // Continue onto scanning.
                    return Ok(Poll::Ready(()));
                }
            }
        }
    }
}
