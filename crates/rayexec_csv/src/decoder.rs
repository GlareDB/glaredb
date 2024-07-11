use csv_core::Reader;
use rayexec_error::{RayexecError, Result, ResultExt};

use crate::reader::DialectOptions;

const DATA_BUFFER_SIZE: usize = 4 * 1024;
const END_BUFFER_SIZE: usize = 1024;

#[derive(Debug)]
pub struct DecoderState {
    /// Buffer containing decoded records.
    buffer: Vec<u8>,

    /// Length of decoded data in `buffer`.
    buffer_len: usize,

    /// End offsets for fields in `buffer`.
    ends: Vec<usize>,

    /// Length of end offsets in `ends`.
    ends_len: usize,

    /// Field index in a record we're currently decoding.
    current_field: usize,

    /// Number of fields we've detected during decoding.
    ///
    /// Only set when we've completed our first record.
    num_fields: Option<usize>,
}

impl Default for DecoderState {
    fn default() -> Self {
        DecoderState {
            buffer: vec![0; DATA_BUFFER_SIZE],
            buffer_len: 0,
            ends: vec![0; END_BUFFER_SIZE],
            ends_len: 0,
            current_field: 0,
            num_fields: None,
        }
    }
}

impl DecoderState {
    /// Get the number of complete records we've decoded so far.
    pub fn num_records(&self) -> usize {
        let fields = match self.num_fields {
            Some(n) => n,
            None => return 0,
        };

        self.ends_len / fields
    }

    pub fn num_fields(&self) -> Option<usize> {
        self.num_fields
    }

    /// Get the buffer offset relative to the current record being written.
    pub fn relative_start_offset(&self) -> usize {
        let num_completed = self.num_records();
        if num_completed == 0 {
            return 0;
        }
        let num_fields = match self.num_fields {
            Some(n) => n,
            None => return 0,
        };

        self.ends[num_completed * num_fields - 1]
    }

    pub fn clear_completed(&mut self) {
        let num_completed = self.num_records();
        let num_fields = match self.num_fields {
            Some(n) => n,
            None => return, // No completed records to clear.
        };

        if self.current_field == 0 {
            // Fast path, we can just reset the buffers.
            self.buffer.clear();
            self.buffer_len = 0;
            self.ends.clear();
            self.ends_len = 0;
            return;
        }

        // Get start index of data that's part of a partial record.
        let start_data_idx = self.ends[num_completed * num_fields - 1];

        // Shift ends down.
        let ends_idx = num_completed * num_fields;
        self.ends
            .copy_within(ends_idx..(ends_idx + self.current_field), 0);

        // Get end data index based on the most recent field we've read.
        let end_data_idx = self.ends[self.current_field - 1];

        // Shift data down.
        self.buffer.copy_within(start_data_idx..end_data_idx, 0);

        self.buffer_len = end_data_idx - start_data_idx;
        self.ends_len = self.current_field;

        // Adjust ends to account for shifted data.
        for end in self.ends.iter_mut().take(self.ends_len) {
            *end -= start_data_idx;
        }
    }

    /// Resets the state to as if we've never decoded anything.
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.buffer_len = 0;
        self.ends.clear();
        self.ends_len = 0;
        self.current_field = 0;
        self.num_fields = None;
    }

    pub fn completed_records(&self) -> CompletedRecords {
        CompletedRecords { state: self }
    }
}

#[derive(Debug)]
pub struct CompletedRecords<'a> {
    state: &'a DecoderState,
}

impl<'a> CompletedRecords<'a> {
    pub fn num_completed(&self) -> usize {
        self.state.num_records()
    }

    pub fn num_fields(&self) -> Option<usize> {
        self.state.num_fields()
    }

    pub fn get_record(&self, idx: usize) -> Option<CompletedRecord<'a>> {
        let num_fields = self.state.num_fields?;
        if idx >= self.state.num_records() {
            return None;
        }

        let ends = &self.state.ends[(idx * num_fields)..(idx * num_fields + num_fields)];
        let data_start = if idx == 0 {
            0
        } else {
            let ends_idx = idx * num_fields - 1;
            self.state.ends[ends_idx]
        };

        Some(CompletedRecord {
            line: idx + 1,
            data: &self.state.buffer,
            data_start,
            ends,
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = CompletedRecord> {
        (0..self.state.num_records()).map(|idx| self.get_record(idx).unwrap())
    }
}

#[derive(Debug)]
pub struct CompletedRecord<'a> {
    line: usize,
    data: &'a [u8],
    data_start: usize,
    ends: &'a [usize],
}

impl<'a> CompletedRecord<'a> {
    pub fn get_field(&self, idx: usize) -> Result<&'a str> {
        let start = if idx == 0 {
            self.data_start
        } else {
            self.ends[idx - 1]
        };
        let end = self.ends[idx];

        // TODO: 'line' in error message isn't line within the file, but within
        // the group of batches. Not useful to the end user.
        std::str::from_utf8(&self.data[start..end]).context_fn(|| {
            format!(
                "Field '{idx}' on line '{}' contains invalid UTF-8 data",
                self.line
            )
        })
    }

    pub fn iter(&self) -> impl Iterator<Item = Result<&str>> {
        (0..self.ends.len()).map(|idx| self.get_field(idx))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderResult {
    /// Decoder received empty input, decoding finished.
    Finished,

    /// Input was completely exhausted.
    InputExhuasted,

    /// Buffer was full but input was not exhuasted. Call `decode` again with a
    /// sliced input beginning at offset to resume.
    BufferFull { input_offset: usize },
}

#[derive(Debug)]
pub struct CsvDecoder {
    reader: Reader,
}

impl CsvDecoder {
    pub fn new(dialect: DialectOptions) -> Self {
        CsvDecoder {
            reader: dialect.csv_core_reader(),
        }
    }

    /// Decode an input buffer writing decoded fields to `state`.
    pub fn decode(&mut self, input: &[u8], state: &mut DecoderState) -> Result<DecoderResult> {
        let mut input_offset = 0;

        // Read as many records as we can.
        loop {
            // Track the relative offset for the record we're currently working
            // on. This is used to adjust the end offsets after reading a record
            // into the buffer.
            let relative_offset = state.relative_start_offset();

            let input = &input[input_offset..];
            let output = &mut state.buffer[state.buffer_len..];
            let ends = &mut state.ends[state.ends_len..];

            let (result, bytes_read, bytes_written, ends_written) =
                self.reader.read_record(input, output, ends);

            input_offset += bytes_read;
            state.buffer_len += bytes_written;
            state.ends_len += ends_written;
            state.current_field += ends_written;

            // Adjust written end offsets to account for storing all records in
            // a single buffer.
            for end in ends.iter_mut().take(ends_written) {
                *end += relative_offset;
            }

            match result {
                csv_core::ReadRecordResult::InputEmpty => return Ok(DecoderResult::InputExhuasted),
                csv_core::ReadRecordResult::OutputFull => {
                    return Ok(DecoderResult::BufferFull { input_offset })
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    return Ok(DecoderResult::BufferFull { input_offset })
                }
                csv_core::ReadRecordResult::Record => {
                    match state.num_fields {
                        Some(num) => {
                            if state.current_field != num {
                                return Err(RayexecError::new(format!(
                                    "Invalid number of fields in record. Got {}, expected {}",
                                    state.current_field, num
                                )));
                            }
                        }
                        None => state.num_fields = Some(state.current_field),
                    }

                    state.current_field = 0;
                    // Continue reading records.
                }
                csv_core::ReadRecordResult::End => return Ok(DecoderResult::Finished),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incomplete_record() {
        // All records must have a trailing newline.

        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut state = DecoderState::default();

        let input = "a,bb,ccc";

        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(0, state.num_records());
        assert_eq!(None, state.num_fields());
    }

    #[test]
    fn completed_single_line() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut state = DecoderState::default();

        let input = "a,bb,ccc\n";

        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(1, state.num_records());
        assert_eq!(Some(3), state.num_fields());
        assert_eq!(6, state.relative_start_offset());

        let fields: Vec<Vec<_>> = state
            .completed_records()
            .iter()
            .map(|r| r.iter().map(|s| s.unwrap().to_string()).collect())
            .collect();

        let expected = vec![vec!["a", "bb", "ccc"]];

        assert_eq!(expected, fields);
    }

    #[test]
    fn completed_single_line_partial_second_line() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut state = DecoderState::default();

        let input = "a,bb,ccc\ndddd,eeeee,";

        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(1, state.num_records());
        assert_eq!(Some(3), state.num_fields());
        assert_eq!(6, state.relative_start_offset());

        let fields: Vec<Vec<_>> = state
            .completed_records()
            .iter()
            .map(|r| r.iter().map(|s| s.unwrap().to_string()).collect())
            .collect();

        let expected = vec![vec!["a", "bb", "ccc"]];
        assert_eq!(expected, fields);

        // Completed second line.
        state.clear_completed();

        assert_eq!(0, state.num_records());
        assert_eq!(Some(3), state.num_fields());
        assert_eq!(0, state.relative_start_offset());

        let input = "ffffff\n";
        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(1, state.num_records());
        assert_eq!(Some(3), state.num_fields());

        let fields: Vec<Vec<_>> = state
            .completed_records()
            .iter()
            .map(|r| r.iter().map(|s| s.unwrap().to_string()).collect())
            .collect();

        let expected = vec![vec!["dddd", "eeeee", "ffffff"]];
        assert_eq!(expected, fields);
    }

    #[test]
    fn multiline() {
        let input = r#"c1,c2,c3
1,mario,2.3
4,wario,5.6
7,peach,8.9
"#;

        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut state = DecoderState::default();

        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(4, state.num_records());
        assert_eq!(Some(3), state.num_fields());
        assert_eq!(33, state.relative_start_offset());

        let expected = vec![
            vec!["c1", "c2", "c3"],
            vec!["1", "mario", "2.3"],
            vec!["4", "wario", "5.6"],
            vec!["7", "peach", "8.9"],
        ];

        let fields: Vec<Vec<_>> = state
            .completed_records()
            .iter()
            .map(|r| r.iter().map(|s| s.unwrap().to_string()).collect())
            .collect();

        assert_eq!(expected, fields);
    }

    #[test]
    fn clear_completed_with_no_partial_records() {
        // Shouldn't panic with no partial records. `clear_completed` is called
        // in all cases.
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut state = DecoderState::default();

        let input = "a,bb,ccc\n";
        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(0, state.current_field);
        assert_eq!(1, state.num_records());

        state.clear_completed();

        assert_eq!(0, state.num_records());
    }

    #[test]
    fn empty_trailing_field() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut state = DecoderState::default();

        let input = "a,bb,ccc,\neee,fff,ggg,hhh\n";

        decoder.decode(input.as_bytes(), &mut state).unwrap();

        assert_eq!(2, state.num_records());
        assert_eq!(Some(4), state.num_fields());
        assert_eq!(18, state.relative_start_offset());

        let fields: Vec<Vec<_>> = state
            .completed_records()
            .iter()
            .map(|r| r.iter().map(|s| s.unwrap().to_string()).collect())
            .collect();

        let expected = vec![vec!["a", "bb", "ccc", ""], vec!["eee", "fff", "ggg", "hhh"]];

        assert_eq!(expected, fields);
    }
}
