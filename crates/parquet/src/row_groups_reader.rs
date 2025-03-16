// #[derive(Debug)]
// pub struct RowGroupsReader {
//     metadata: Arc<ParquetMetaData>,
//     /// File source we're reading from.
//     source: Box<dyn FileSource>,
//     /// Current group state.
//     group_state: GroupState,
//     /// Next row groups to read from.
//     row_groups: VecDeque<usize>,
//     /// Current fetch state.
//     fetch_state: FetchState,
//     /// Projected columns from the parquet file.
//     projections: Projections,
//     /// The reader for the root of the schema.
//     root: StructReader,
// }

// #[derive(Debug, Clone, Copy)]
// struct GroupState {
//     /// Index of the current group we're scanning.
//     ///
//     /// The initial value for this doesn't matter as we'll immediately try to
//     /// get the next row group from the queue.
//     current_group: usize,
//     /// Remaining number of rows in the group.
//     ///
//     /// This gets set when we read in a new group, and continuously updated as
//     /// we read values.
//     ///
//     /// Once this reaches zero, the next row group should be fetched.
//     remaining_group_rows: usize,
// }

// #[derive(Debug)]
// enum FetchState {
//     /// We need to fetch data for a column.
//     NeedsFetch {
//         /// Column we need to fetch data for.
//         ///
//         /// Relative to the output projection.
//         column_idx: usize,
//     },
//     /// We're currently fetching a column.
//     Fetching {
//         /// Column we're reading for.
//         ///
//         /// This index is relative to the output projection.
//         column_idx: usize,
//         /// Amount written to the columns buffer.
//         amount_written: usize,
//         /// Stream used for pulling data from the source.
//         stream: Pin<Box<dyn AsyncReadStream>>,
//     },
//     /// All column chunks for this row group have been fetched, we can continue
//     /// scanning.
//     Fetched,
// }

// impl RowGroupsReader {
//     pub fn poll_pull(&mut self, cx: &mut Context, output: &mut Batch) -> Result<PollPull> {
//         if self.group_state.remaining_group_rows == 0 {
//             // No more rows in this group (or we haven't even begun scanning).
//             // Get the next row group.
//             let next = match self.row_groups.pop_front() {
//                 Some(next) => next,
//                 None => {
//                     // No more row groups, exhausted.
//                     output.set_num_rows(0)?;
//                     return Ok(PollPull::Exhausted);
//                 }
//             };

//             let row_group = self.metadata.row_group(next);
//             self.group_state = GroupState {
//                 current_group: next,
//                 remaining_group_rows: row_group.num_rows() as usize,
//             };

//             // Set fetch state to init fetching the first column, triggering all
//             // subsequent column fetches for this new row group.
//             self.fetch_state = FetchState::NeedsFetch { column_idx: 0 }
//         }

//         match self.poll_fetch(cx)? {
//             Poll::Ready(_) => (), // Good to go, we have all column chunks.
//             Poll::Pending => return Ok(PollPull::Pending), // Pending read, state updated to resume fetching of a column chunk.
//         }
//         debug_assert!(matches!(self.fetch_state, FetchState::Fetched));

//         // Now we can scan. No async nonsense happens after this point.

//         let cap = output.write_capacity()?;
//         let count = usize::min(self.group_state.remaining_group_rows, cap);

//         debug_assert_eq!(output.arrays().len(), self.root.readers.len());

//         for (output, col_reader) in output.arrays_mut().iter_mut().zip(&mut self.root.readers) {
//             col_reader.read(output, count)?;
//             self.group_state.remaining_group_rows -= count;
//         }

//         output.set_num_rows(count)?;

//         Ok(PollPull::HasMore)
//     }

//     /// Fetches data for all columns in the row group.
//     ///
//     /// This will update the page readers for each column to hold its new column
//     /// chunk.
//     ///
//     /// Fetch happens async. The state of this read will track the current
//     /// column chunk we're fetching, and will pick up where it left off on the
//     /// next poll.
//     ///
//     /// This may be called even if all column chunks have been fetched. The
//     /// internal state will just make this immediately return.
//     fn poll_fetch(&mut self, cx: &mut Context) -> Result<Poll<()>> {
//         let row_group = self.metadata.row_group(self.group_state.current_group);
//         loop {
//             match &mut self.fetch_state {
//                 FetchState::NeedsFetch { column_idx } => {
//                     // Init fetch stream.
//                     let col = row_group.column(self.projections.column_indices[*column_idx]);

//                     let (start, len) = col.byte_range();
//                     let stream = self.source.read_range(start as usize, len as usize);

//                     // Reset and resize this column's buffer.
//                     let reader = &mut self.root.readers[*column_idx];
//                     reader.page_reader.chunk_offset = 0;
//                     reader.page_reader.chunk.reserve_for_size(len as usize)?;

//                     self.fetch_state = FetchState::Fetching {
//                         column_idx: *column_idx,
//                         amount_written: 0,
//                         stream,
//                     }
//                 }
//                 FetchState::Fetching {
//                     column_idx,
//                     amount_written,
//                     stream,
//                 } => {
//                     // Read into this column's buffer. Buffer should be the
//                     // correct size.
//                     let buf = self.root.readers[*column_idx]
//                         .page_reader
//                         .chunk
//                         .as_slice_mut();

//                     // Resume the read.
//                     let mut read = ReadInto::resume(stream, buf, *amount_written);
//                     let poll = read.poll_unpin(cx);
//                     match poll {
//                         Poll::Ready(Ok(count)) => {
//                             // Read the entire chunk. Move to the next column.
//                             if count != buf.len() {
//                                 return Err(RayexecError::new(
//                                     "Read fewer bytes than expected for this chunk",
//                                 )
//                                 .with_field("expected", buf.len())
//                                 .with_field("read", count));
//                             }

//                             if *column_idx == self.root.readers.len() - 1 {
//                                 // All columns fetched.
//                                 self.fetch_state = FetchState::Fetched;
//                                 continue;
//                             }

//                             // Otherwise begin fetching the next column.
//                             self.fetch_state = FetchState::NeedsFetch {
//                                 column_idx: *column_idx + 1,
//                             }
//                         }
//                         Poll::Ready(Err(e)) => return Err(e),
//                         Poll::Pending => {
//                             // Fetch pending, update our state to resume
//                             // fetching on the next poll.
//                             *amount_written = read.amount_written();

//                             return Ok(Poll::Pending);
//                         }
//                     }
//                 }
//                 FetchState::Fetched => {
//                     // Continue on to scanning.
//                     return Ok(Poll::Ready(()));
//                 }
//             }
//         }
//     }
// }
