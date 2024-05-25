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
    pub completed: usize,

    /// The offset that should be used in the next call to `decode`.
    pub input_offset: usize,
}

/// State for decoding records from a csv input.
#[derive(Debug)]
pub struct Decoder {
    /// Configured csv reader.
    csv_reader: Reader,

    /// Buffered data decoded from the input.
    buffer: Vec<u8>,

    /// Current buffer length.
    buffer_len: usize,

    /// Offsets into the buffer for individual fields in a record.
    offsets: Vec<usize>,

    /// Current offsets length.
    offsets_len: usize,

    /// Number of fields we should expect to read from the input.
    num_fields: Option<usize>,

    /// Index of the current field we're trying to read.
    ///
    /// This is used to assert that all records has the same number of fields.
    current_field: usize,

    /// Number of records read in full so far.
    completed: usize,
}

impl Decoder {
    pub fn new(csv_reader: Reader, num_fields: Option<usize>) -> Self {
        Decoder {
            csv_reader,
            buffer: vec![0; START_BUFFER_CAP],
            buffer_len: 0,
            offsets: vec![0; START_ENDS_CAP],
            offsets_len: 1, // First value is always 0.
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
            // Buffers to pass to read.
            let input = &input[input_offset..];
            let output = &mut self.buffer[self.buffer_len..];
            let ends = &mut self.offsets[self.offsets_len..];

            println!("--- input: {}", std::str::from_utf8(input).unwrap());

            // Try to read a complete record.
            let (result, bytes_read, bytes_written, ends_written) =
                self.csv_reader.read_record(input, output, ends);

            input_offset += bytes_read;
            self.buffer_len += bytes_written;
            self.offsets_len += ends_written;
            self.current_field += ends_written;

            match result {
                ReadRecordResult::End => {
                    // We've read a complete set of records. Reset the reader to
                    // allow decoding additional records.
                    self.csv_reader.reset();

                    return Ok(DecodeResult {
                        completed,
                        input_offset,
                    });
                }
                ReadRecordResult::InputEmpty => {
                    // We have a partially decoded record.
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
                    self.offsets
                        .resize(self.offsets.len() + ENDS_CAP_INCREMENT, 0);
                }
                ReadRecordResult::Record => {
                    println!("num: {}", self.current_field);
                    match self.num_fields {
                        Some(num_fields) => {
                            if self.current_field != num_fields {
                                println!(
                                    "--- buf: {}",
                                    std::str::from_utf8(&self.buffer[..self.buffer_len]).unwrap()
                                );
                                return Err(RayexecError::new(format!(
                                    "Invalid number of fields in record. Got {}, expected {}",
                                    self.current_field, num_fields
                                )));
                            }
                        }
                        None => self.num_fields = Some(self.current_field),
                    }

                    self.completed += 1;
                    self.current_field = 0;

                    completed += 1;

                    // Continue on trying to read more from the input.
                }
            }
        }
    }

    pub fn num_fields(&self) -> Option<usize> {
        self.num_fields
    }

    /// Flush out the decoded records.
    pub fn flush(&mut self) -> Result<DecodedRecords> {
        if self.current_field != 0 {
            return Err(RayexecError::new(
                "Cannot flush with partially decoded records",
            ));
        }

        // Adjust offsets as the csv reader writes offsets relative to the
        // buffer we pass in, which isn't the full buffer.
        let mut row_offset = 0;
        self.offsets[1..self.offsets_len]
            .chunks_exact_mut(self.num_fields.unwrap())
            .for_each(|row| {
                let offset = row_offset;
                row.iter_mut().for_each(|x| {
                    *x += offset;
                    row_offset = *x;
                });
            });

        let data = std::str::from_utf8(&self.buffer[0..self.buffer_len])
            .map_err(|_| RayexecError::new("Invalid UTF-8 data in buffer"))?;

        let num_records = self.completed;

        // Reset
        self.offsets_len = 1;
        self.buffer_len = 0;
        self.completed = 0;

        Ok(DecodedRecords {
            num_records,
            num_fields: self.num_fields.unwrap(),
            data,
            offsets: &self.offsets,
        })
    }
}

#[derive(Debug)]
pub struct DecodedRecords<'a> {
    num_records: usize,
    num_fields: usize,
    data: &'a str,
    offsets: &'a [usize],
}

impl<'a> DecodedRecords<'a> {
    /// Get a single record from the decoded records.
    pub fn get_record(&self, idx: usize) -> DecodedRecord {
        assert!(idx < self.num_records);
        let offset_idx = idx * self.num_fields;
        let offsets = &self.offsets[offset_idx..offset_idx + self.num_fields + 1];

        DecodedRecord {
            data: self.data,
            offsets,
            num_fields: self.num_fields,
        }
    }

    pub fn num_fields(&self) -> usize {
        self.num_fields
    }

    pub fn num_records(&self) -> usize {
        self.num_records
    }

    /// Get an iterator over the records.
    pub fn iter(&self) -> impl Iterator<Item = DecodedRecord> {
        (0..self.num_records).map(|idx| self.get_record(idx))
    }
}

#[derive(Debug)]
pub struct DecodedRecord<'a> {
    data: &'a str,
    offsets: &'a [usize],
    num_fields: usize,
}

impl<'a> DecodedRecord<'a> {
    pub fn get_field(&self, idx: usize) -> &'a str {
        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];

        self.data.get(start..end).unwrap()
    }

    /// Get an iterator over the fields in the record.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
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
        let mut decoder = Decoder::new(reader, Some(3));

        // TODO: Maybe don't require a final new line.
        let input = ["1,2,3", "\"aaa\",\"bbb\",\"ccc\"", ",,", ""].join("\n");

        let res = decoder.decode(input.as_bytes()).unwrap();
        assert_eq!(3, res.completed);

        let decoded = decoder.flush().unwrap();

        let record1 = decoded.get_record(0);
        let fields1: Vec<_> = record1.iter().collect();
        assert_eq!(vec!["1", "2", "3"], fields1);

        let record2 = decoded.get_record(1);
        let fields2: Vec<_> = record2.iter().collect();
        assert_eq!(vec!["aaa", "bbb", "ccc"], fields2);

        let record3 = decoded.get_record(2);
        let fields3: Vec<_> = record3.iter().collect();
        assert_eq!(vec!["", "", ""], fields3);
    }

    #[test]
    fn multiple_decode() {
        let reader = ReaderBuilder::new().build();
        let mut decoder = Decoder::new(reader, Some(3));

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

        let decoded = decoder.flush().unwrap();

        let record1 = decoded.get_record(0);
        let fields1: Vec<_> = record1.iter().collect();
        assert_eq!(vec!["1", "2", "3"], fields1);

        let record2 = decoded.get_record(1);
        let fields2: Vec<_> = record2.iter().collect();
        assert_eq!(vec!["aaa", "bbb", "ccc"], fields2);

        let record3 = decoded.get_record(2);
        let fields3: Vec<_> = record3.iter().collect();
        assert_eq!(vec!["", "", ""], fields3);
    }
}
