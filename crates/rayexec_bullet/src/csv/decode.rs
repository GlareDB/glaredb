use csv_core::{ReadRecordResult, Reader};
use rayexec_error::{RayexecError, Result};

const START_BUFFER_CAP: usize = 512;
const BUFFER_CAP_INCREMENT: usize = 512;

const START_ENDS_CAP: usize = 48;
const ENDS_CAP_INCREMENT: usize = 48;

/// Result of a decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DecodeResult {
    /// Number of records completely decoded.
    ///
    /// This may contain a record that was partially decoded in a previous call
    /// to `decode`.
    completed: usize,

    /// The offset that should be used in the next call to `decode`.
    input_offset: usize,
}

/// State for decoding records from a csv input.
#[derive(Debug)]
pub struct Decoder {
    /// Configured csv reader.
    csv_reader: Reader,

    /// Buffered data decoded from the input.
    buffer: Vec<u8>,

    /// Current offset into `buffer`.
    buffer_offset: usize,

    /// End offsets into the buffer for individual fields in a record.
    ends: Vec<usize>,

    /// Current offset into `ends`.
    ends_offset: usize,

    /// Number of fields we should expect to read from the input.
    num_fields: usize,

    /// Index of the current field we're trying to read.
    ///
    /// This is used to assert that all records has the same number of fields.
    current_field: usize,

    /// Number of records read in full so far.
    completed: usize,
}

impl Decoder {
    pub fn new(csv_reader: Reader, num_fields: usize) -> Self {
        Decoder {
            csv_reader,
            buffer: vec![0; START_BUFFER_CAP],
            buffer_offset: 0,
            ends: vec![0; START_ENDS_CAP],
            ends_offset: 0,
            num_fields,
            current_field: 0,
            completed: 0,
        }
    }

    /// Decode some number of records from a byte slice.
    pub fn decode(&mut self, input: &[u8]) -> Result<DecodeResult> {
        // Offset into input. A single call to `decode` may read multiple
        // records.
        let mut input_offset = 0;

        let mut completed = 0;

        // Read as many records as we can.
        loop {
            let input = &input[input_offset..];
            let output = &mut self.buffer[self.buffer_offset..];
            let ends = &mut self.ends[self.ends_offset..];

            // Try to read a complete record.
            let (result, bytes_read, bytes_written, ends_written) =
                self.csv_reader.read_record(input, output, ends);

            input_offset += bytes_read;
            self.buffer_offset += bytes_written;
            self.ends_offset += ends_written;
            self.current_field += ends_written;

            match result {
                ReadRecordResult::InputEmpty | ReadRecordResult::End => {
                    // We're done.
                    return Ok(DecodeResult {
                        completed,
                        input_offset,
                    });
                }
                ReadRecordResult::OutputFull => {
                    // Resize output buffer, and continue trying to read.
                    self.buffer
                        .resize(self.buffer.len() + BUFFER_CAP_INCREMENT, 0);
                }
                ReadRecordResult::OutputEndsFull => {
                    // Resize 'ends' buffer, continue trying to read.
                    self.ends.resize(self.ends.len() + ENDS_CAP_INCREMENT, 0);
                }
                ReadRecordResult::Record => {
                    if self.current_field != self.num_fields {
                        return Err(RayexecError::new(format!(
                            "Invalid number of fields in record. Got {}, expected {}",
                            self.current_field, self.num_fields
                        )));
                    }

                    self.completed += 1;
                    self.current_field = 0;

                    completed += 1;

                    if input.len() == input_offset {
                        return Ok(DecodeResult {
                            completed,
                            input_offset,
                        });
                    }

                    // Continue on trying to read more from the input.
                }
            }
        }
    }

    /// Get a view into the decoded records.
    pub fn decoded_records(&self) -> DecodedRecords {
        DecodedRecords {
            num_records: self.completed,
            num_fields: self.num_fields,
            buffer: &self.buffer,
            ends: &self.ends,
        }
    }

    /// Clears the decoder state.
    ///
    /// Note that this will also clear a partially decoded record.
    pub fn clear(&mut self) {
        self.completed = 0;
        self.current_field = 0;
        self.buffer_offset = 0;
        self.ends_offset = 0;
        self.csv_reader.reset();
    }
}

#[derive(Debug)]
pub struct DecodedRecords<'a> {
    num_records: usize,
    num_fields: usize,
    buffer: &'a [u8],
    ends: &'a [usize],
}

impl<'a> DecodedRecords<'a> {
    /// Get a single record from the decoded records.
    pub fn get_record(&self, idx: usize) -> DecodedRecord {
        assert!(idx < self.num_records);
        // Get the slice of the buffer such that index '0' is the start of this
        // record.
        let buffer = if idx == 0 {
            &self.buffer
        } else {
            let start = self.ends[(idx * self.num_fields) - 1];
            &self.buffer[start..]
        };
        let ends = &self.ends[(idx * self.num_fields)..];

        DecodedRecord {
            buffer,
            ends,
            num_fields: self.num_fields,
        }
    }

    /// Get an iterator over the records.
    pub fn iter(&self) -> impl Iterator<Item = DecodedRecord> {
        (0..self.num_records).map(|idx| self.get_record(idx))
    }
}

#[derive(Debug)]
pub struct DecodedRecord<'a> {
    buffer: &'a [u8],
    ends: &'a [usize],
    num_fields: usize,
}

impl<'a> DecodedRecord<'a> {
    pub fn get_field(&self, idx: usize) -> Result<&'a str> {
        let start = if idx == 0 { 0 } else { self.ends[idx - 1] };
        let end = self.ends[idx];
        let bs = &self.buffer[start..end];

        std::str::from_utf8(bs).map_err(|_| RayexecError::new("Field is not valid UTF-8"))
    }

    /// Get an iterator over the fields in the record.
    pub fn iter(&self) -> impl Iterator<Item = Result<&str>> {
        (0..self.num_fields).map(|idx| self.get_field(idx))
    }
}

#[cfg(test)]
mod tests {
    use csv_core::ReaderBuilder;

    use super::*;

    #[test]
    fn simple() {
        let reader = ReaderBuilder::new().build();
        let mut decoder = Decoder::new(reader, 3);

        // TODO: Maybe don't require a final new line.
        let input = ["1,2,3", "\"aaa\",\"bbb\",\"ccc\"", ",,", ""].join("\n");

        let res = decoder.decode(input.as_bytes()).unwrap();
        assert_eq!(3, res.completed);

        let decoded = decoder.decoded_records();

        let record1 = decoded.get_record(0);
        let fields1 = record1.iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(vec!["1", "2", "3"], fields1);

        let record2 = decoded.get_record(1);
        let fields2 = record2.iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(vec!["aaa", "bbb", "ccc"], fields2);

        let record3 = decoded.get_record(2);
        let fields3 = record3.iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(vec!["", "", ""], fields3);
    }

    #[test]
    fn multiple_decode() {
        let reader = ReaderBuilder::new().build();
        let mut decoder = Decoder::new(reader, 3);

        let input = ["1,2,3", "\"aaa\",\"bbb\",\"ccc\"", ",,"];

        let mut read = 0;
        for line in input {
            let res = decoder.decode(line.as_bytes()).unwrap();
            read += res.completed;
            assert_eq!(line.as_bytes().len(), res.input_offset);

            let res = decoder.decode(&[b'\n']).unwrap();
            read += res.completed;
            assert_eq!(1, res.input_offset);
        }

        assert_eq!(3, read);

        let decoded = decoder.decoded_records();

        let record1 = decoded.get_record(0);
        let fields1 = record1.iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(vec!["1", "2", "3"], fields1);

        let record2 = decoded.get_record(1);
        let fields2 = record2.iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(vec!["aaa", "bbb", "ccc"], fields2);

        let record3 = decoded.get_record(2);
        let fields3 = record3.iter().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(vec!["", "", ""], fields3);
    }
}
