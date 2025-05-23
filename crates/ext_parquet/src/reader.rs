use std::collections::VecDeque;
use std::io;
use std::sync::Arc;
use std::task::{Context, Poll};

use glaredb_core::arrays::batch::Batch;
use glaredb_core::arrays::field::ColumnSchema;
use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::execution::operators::PollPull;
use glaredb_core::runtime::filesystem::AnyFile;
use glaredb_core::runtime::filesystem::file_provider::MultiFileProvider;
use glaredb_core::storage::projections::{ProjectedColumn, Projections};
use glaredb_core::storage::scan_filter::PhysicalScanFilter;
use glaredb_error::{DbError, Result};

use crate::column::struct_reader::StructReader;
use crate::metadata::ParquetMetaData;
use crate::schema::types::SchemaDescriptor;

/// A unit of scanning.
#[derive(Debug)]
pub struct ScanUnit {
    /// Metadata for the file we're currently reading.
    pub metadata: Arc<ParquetMetaData>,
    /// File we're reading from.
    pub file: AnyFile,
    /// Queue of row groups to read from.
    pub row_groups: VecDeque<usize>,
}

#[derive(Debug)]
pub struct Reader {
    unit: Option<ScanUnit>,
    /// State of the current row group we're reading.
    state: RowGroupState,
    /// State for the fetching data from the file.
    fetch_state: FetchState,
    /// Column projections for this file.
    projections: Projections, // TODO: Possibly arc
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
    // TODO: Need parquet specific schema
    pub fn try_new(
        manager: &impl AsRawBufferManager,
        schema: ColumnSchema,
        parquet_schema: &SchemaDescriptor,
        projections: Projections,
        filters: &[PhysicalScanFilter],
    ) -> Result<Self> {
        let root =
            StructReader::try_new_root(manager, &projections, &schema, parquet_schema, filters)?;

        Ok(Reader {
            unit: None,
            state: RowGroupState {
                current_group: 0,
                remaining_group_rows: 0, // Will trigger getting the real first row group.
            },
            fetch_state: FetchState::NeedsFetch { column_idx: 0 }, // This doesn't matter here.
            projections,
            root,
        })
    }

    /// Preprates this reader to begin reading from a new scan unit.
    pub fn prepare(&mut self, unit: ScanUnit) {
        self.unit = Some(unit);
        self.state = RowGroupState {
            current_group: 0,
            remaining_group_rows: 0,
        };
        self.fetch_state = FetchState::NeedsFetch { column_idx: 0 };
    }

    /// Scan rows into the output batch.
    ///
    /// This will fetch column chunks as needed.
    pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
        let unit = match self.unit.as_mut() {
            Some(unit) => unit,
            None => {
                return Err(DbError::new(
                    "Attempted to pull from parquet reader without preparing a read unit",
                ));
            }
        };

        if self.state.remaining_group_rows == 0 {
            // No more rows, get the next row group.
            loop {
                let next = match unit.row_groups.pop_front() {
                    Some(group) => group,
                    None => {
                        // No more row groups, we're done.
                        output.set_num_rows(0)?;
                        return Ok(PollPull::Exhausted);
                    }
                };

                let group = &unit.metadata.row_groups[next];
                // We may be able to prune this group...
                if self.root.should_prune(&self.projections, group)? {
                    // Safe to prune, move to next row group.
                    continue;
                }

                self.state = RowGroupState {
                    current_group: next,
                    remaining_group_rows: group.num_rows as usize,
                };

                // Init fetch.
                self.fetch_state = FetchState::NeedsFetch { column_idx: 0 };
                // Continue on... The fetch state will trigger fetching of the
                // columns.
                break;
            }
        }

        match self.poll_fetch(cx)? {
            Poll::Ready(_) => (), // Good to go, we have all column chunks.
            Poll::Pending => return Ok(PollPull::Pending), // Pending read, state updated to resume fetching of a column chunk.
        }
        debug_assert!(matches!(self.fetch_state, FetchState::Fetched));

        // Now we can scan. No async nonsense happens after this point.

        let cap = output.write_capacity()?;
        let count = usize::min(self.state.remaining_group_rows, cap);

        // Handle data columns.
        let data_arrays = &mut output.arrays_mut()[..self.projections.data_indices().len()];
        debug_assert_eq!(data_arrays.len(), self.root.readers.len());

        for (arr, reader) in data_arrays.iter_mut().zip(&mut self.root.readers) {
            reader.read(arr, count)?;
        }

        // Handle metadata columns.
        let meta_arrays = &mut output.arrays_mut()[self.projections.data_indices().len()..];
        debug_assert_eq!(meta_arrays.len(), self.projections.meta_indices().len());

        for (&meta_idx, arr) in self.projections.meta_indices().iter().zip(meta_arrays) {
            match meta_idx {
                MultiFileProvider::META_PROJECTION_FILENAME => {
                    // Hello!
                }
                MultiFileProvider::META_PROJECTION_ROWID => {
                    // Hello!
                }
                other => panic!("invalid meta projection: {other}"),
            }
        }

        self.state.remaining_group_rows -= count;
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
        let unit = self.unit.as_mut().expect("Unit to be set");

        let row_group = &unit.metadata.row_groups[self.state.current_group];
        loop {
            match &mut self.fetch_state {
                FetchState::NeedsFetch { column_idx } => {
                    if self.root.readers.is_empty() {
                        // We're not actually trying to read any columns.
                        // Indicate the we've successfully "fetched" no columns.
                        self.fetch_state = FetchState::Fetched;
                        continue;
                    }

                    let real_idx = self.projections.data_indices()[*column_idx];
                    let col = &row_group.columns[real_idx];

                    let (start, len) = col.byte_range();

                    // Reset and resize this column's buffer.
                    let reader = &mut self.root.readers[*column_idx];
                    reader.prepare_for_chunk(len as usize, col.compression)?;

                    // Begin seeking to the start of this column chunk.
                    self.fetch_state = FetchState::Seeking {
                        seek: io::SeekFrom::Start(start),
                        column_idx: *column_idx,
                    };
                    // Continue...
                }
                FetchState::Seeking { seek, column_idx } => {
                    // Seek in the file.
                    match unit.file.call_poll_seek(cx, *seek) {
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
                    let buf = self.root.readers[*column_idx].chunk_buf_mut();

                    let read_buf = &mut buf[*amount_written..];
                    match unit.file.call_poll_read(cx, read_buf) {
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
