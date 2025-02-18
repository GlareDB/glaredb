use std::{cmp, fmt};

use csv_core::Reader;
use rayexec_error::{RayexecError, Result, ResultExt};

use crate::dialect::DialectOptions;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecoderResult {
    /// Input was exhausted, need more.
    ///
    /// The output may have a partial record written to it.
    NeedsMore,
    /// We finished on a record boundary.
    RecordBoundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecordBoundary {
    /// Index into the `ends` buffer indicating the last field in the record.
    end_idx: usize,
    /// Offset in the output buffer indicating the end of a record.
    end_offset: usize,
}

pub struct ByteRecords {
    /// Decoded fields stored contiguously.
    buf: Vec<u8>,
    buf_len: usize,
    /// Offsets to field ends in the buffer.
    ends: Vec<usize>,
    ends_len: usize,
    record_boundaries: Vec<RecordBoundary>,
}

impl fmt::Debug for ByteRecords {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ByteRecords")
            .field("buf", &self.buf)
            .field(
                "buf_str",
                &std::str::from_utf8(&self.buf).unwrap_or_default(),
            )
            .field("buf_len", &self.buf_len)
            .field("ends", &self.ends)
            .field("ends_len", &self.ends_len)
            .field("record_boundaries", &self.record_boundaries)
            .finish()
    }
}

impl ByteRecords {
    pub fn with_buffer_capacity(cap: usize) -> Self {
        ByteRecords {
            buf: Vec::with_capacity(cap),
            buf_len: 0,
            ends: Vec::new(),
            ends_len: 0,
            record_boundaries: Vec::new(),
        }
    }

    pub fn num_records(&self) -> usize {
        self.record_boundaries.len()
    }

    /// Clear completed and partial records.
    pub fn clear_all(&mut self) {
        self.buf_len = 0;
        self.ends_len = 0;
        self.record_boundaries.clear();
    }

    /// Clears compled records from the buffer.
    ///
    /// Partially read records will be preserved.
    pub fn clear_completed(&mut self) {
        let last = match self.record_boundaries.last() {
            Some(last) => last,
            None => {
                // We have no completed records.
                return;
            }
        };

        if last.end_offset == self.buf_len {
            // We only have completed records. Just clear everything.
            self.buf_len = 0;
            self.ends_len = 0;
            self.record_boundaries.clear();

            return;
        }

        // We have a partial record. Need to move things around.

        // Move the trailing part of the buffer to the beginning.
        self.buf.copy_within(last.end_offset.., 0);
        self.buf_len -= last.end_offset;

        // Move the trailing ends to the beginning. Note we skip the end idx
        // from the last completed buffer. We don't want it. And we guaranteed
        // to have at least one more since we checked above.
        self.ends.copy_within(last.end_idx + 1.., 0);
        self.ends_len -= last.end_idx + 1;

        // Still want to clear this, only contains completed records.
        self.record_boundaries.clear();
    }

    pub fn get_record(&self, idx: usize) -> ByteRecord {
        let (buf, ends) = if idx == 0 {
            let rec_boundary = self.record_boundaries[idx];
            let buf = &self.buf[0..rec_boundary.end_offset];
            // Include the the last end for this record.
            let ends = &self.ends[0..=rec_boundary.end_idx];
            (buf, ends)
        } else {
            let rec_boundary = self.record_boundaries[idx];
            let prev_rec_boundary = self.record_boundaries[idx - 1];

            let buf = &self.buf[prev_rec_boundary.end_offset..rec_boundary.end_offset];
            // Skip the last end index from the previous record, include our
            // last index.
            let ends = &self.ends[prev_rec_boundary.end_idx + 1..=rec_boundary.end_idx];

            (buf, ends)
        };

        ByteRecord { buf, ends }
    }

    pub fn iter_records(&self) -> impl Iterator<Item = ByteRecord> + '_ {
        (0..self.num_records()).map(|idx| self.get_record(idx))
    }

    fn expand_buf(&mut self) {
        let new_len = self.buf.len() * 2;
        self.buf.resize(cmp::max(4, new_len), 0);
    }

    fn expand_ends(&mut self) {
        let new_len = self.ends.len() * 2;
        self.ends.resize(cmp::max(4, new_len), 0);
    }
}

#[derive(Debug)]
pub struct ByteRecord<'a> {
    buf: &'a [u8],
    ends: &'a [usize],
}

impl<'a> ByteRecord<'a> {
    pub fn num_fields(&self) -> usize {
        self.ends.len()
    }

    pub fn field(&self, idx: usize) -> &[u8] {
        if idx == 0 {
            &self.buf[0..self.ends[0]]
        } else {
            &self.buf[self.ends[idx - 1]..self.ends[idx]]
        }
    }

    pub fn iter_fields(&self) -> FieldIter {
        FieldIter {
            offset: 0,
            buf: self.buf,
            ends: self.ends,
        }
    }
}

#[derive(Debug)]
pub struct FieldIter<'a> {
    offset: usize,
    buf: &'a [u8],
    ends: &'a [usize],
}

impl<'a> Iterator for FieldIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.ends.is_empty() {
            return None;
        }

        let field = &self.buf[self.offset..self.ends[0]];

        self.offset = self.ends[0];
        self.ends = &self.ends[1..];

        Some(field)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.ends.len();
        (rem, Some(rem))
    }
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

    /// Decode an input buffer, writing the fields to `output`.
    pub fn decode(&mut self, input: &[u8], output: &mut ByteRecords) -> DecoderResult {
        let mut input_offset = 0;
        loop {
            let input = &input[input_offset..];

            let (result, bytes_read, bytes_written, ends_written) = self.reader.read_record(
                input,
                &mut output.buf[output.buf_len..],
                &mut output.ends[output.ends_len..],
            );

            input_offset += bytes_read;

            output.buf_len += bytes_written;
            output.ends_len += ends_written;

            match result {
                csv_core::ReadRecordResult::InputEmpty => return DecoderResult::NeedsMore,
                csv_core::ReadRecordResult::OutputFull => {
                    output.expand_buf();
                    continue;
                }
                csv_core::ReadRecordResult::OutputEndsFull => {
                    output.expand_ends();
                    continue;
                }
                csv_core::ReadRecordResult::Record => {
                    output.record_boundaries.push(RecordBoundary {
                        end_idx: output.ends_len - 1,
                        end_offset: output.buf_len,
                    });
                    continue;
                }
                csv_core::ReadRecordResult::End => return DecoderResult::RecordBoundary,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_records_clear_no_partial_records() {
        let mut records = ByteRecords {
            buf: b"mario9.58000\0\0\0\0".to_vec(),
            buf_len: 12,
            ends: vec![5, 8, 12, 0],
            ends_len: 3,
            record_boundaries: vec![RecordBoundary {
                end_idx: 2,
                end_offset: 12,
            }],
        };

        records.clear_completed();
        assert_eq!(0, records.buf_len);
    }

    #[test]
    fn byte_records_clear_with_partial_record() {
        // Next record starts with "wario" but our input buffer only contained
        // the first character. We need to ensure we retain that character.
        let mut records = ByteRecords {
            buf: b"mario9.58000w\0\0\0".to_vec(),
            buf_len: 13,
            ends: vec![5, 8, 12, 0],
            ends_len: 3,
            record_boundaries: vec![RecordBoundary {
                end_idx: 2,
                end_offset: 12,
            }],
        };

        records.clear_completed();
        assert_eq!(1, records.buf_len);
    }

    #[test]
    fn incomplete_record_no_newline() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::NeedsMore, res);

        assert_eq!(0, output.num_records());
    }

    #[test]
    fn completed_single_line() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc\n";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        assert_eq!(1, output.num_records());

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        assert_eq!(expected, fields);
    }

    #[test]
    fn completed_multiple_lines() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc\ndddd,eeeee,ffffff\n";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        assert_eq!(2, output.num_records());

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        assert_eq!(expected, fields);

        let record = output.get_record(1);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"dddd", b"eeeee", b"ffffff"];
        assert_eq!(expected, fields);
    }

    #[test]
    fn completed_line_with_partial_second_line() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc\ndddd,eeeee,";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::NeedsMore, res);

        assert_eq!(1, output.num_records());

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        assert_eq!(expected, fields);

        let rem_input = "ffffff\n";
        let res = decoder.decode(rem_input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        let record = output.get_record(1);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"dddd", b"eeeee", b"ffffff"];
        assert_eq!(expected, fields);
    }

    #[test]
    fn completed_line_with_partial_second_line_incomplete_first_record() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc\ndd";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::NeedsMore, res);

        assert_eq!(1, output.num_records());

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        assert_eq!(expected, fields);

        let rem_input = "dd,eeeee,ffffff\n";
        let res = decoder.decode(rem_input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        let record = output.get_record(1);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"dddd", b"eeeee", b"ffffff"];
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
        let mut output = ByteRecords::with_buffer_capacity(16);

        decoder.decode(input.as_bytes(), &mut output);

        assert_eq!(4, output.num_records());

        let expected: Vec<[&[u8]; 3]> = vec![
            [b"c1", b"c2", b"c3"],
            [b"1", b"mario", b"2.3"],
            [b"4", b"wario", b"5.6"],
            [b"7", b"peach", b"8.9"],
        ];

        for (idx, record) in output.iter_records().enumerate() {
            let fields: Vec<_> = record.iter_fields().collect();
            assert_eq!(&expected[idx], fields.as_slice(), "idx: {idx}");
        }
    }

    #[test]
    fn multiline_zero_width_records() {
        let input = r#",,
,,
,,
,,
    "#;

        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        decoder.decode(input.as_bytes(), &mut output);

        assert_eq!(4, output.num_records());

        let expected: Vec<[&[u8]; 3]> = vec![
            [b"", b"", b""],
            [b"", b"", b""],
            [b"", b"", b""],
            [b"", b"", b""],
        ];

        for (idx, record) in output.iter_records().enumerate() {
            let fields: Vec<_> = record.iter_fields().collect();
            assert_eq!(&expected[idx], fields.as_slice(), "idx: {idx}");
        }
    }

    #[test]
    fn output_clear_with_completed() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc\n";
        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        output.clear_completed();
        assert_eq!(0, output.num_records());

        let input = "dddd,eeeee,ffffff\n";
        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"dddd", b"eeeee", b"ffffff"];
        assert_eq!(expected, fields);
    }

    #[test]
    fn output_clear_preserve_partition() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = "a,bb,ccc\ndddd,eeeee,";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::NeedsMore, res);
        output.clear_completed();

        let input = "ffffff\n";
        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        assert_eq!(1, output.num_records());

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"dddd", b"eeeee", b"ffffff"];
        assert_eq!(expected, fields);
    }

    #[test]
    fn clear_with_zero_width_records() {
        let mut decoder = CsvDecoder::new(DialectOptions::default());
        let mut output = ByteRecords::with_buffer_capacity(16);

        let input = ",bb,\ndddd,,";

        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::NeedsMore, res);
        output.clear_completed();

        let input = "ffffff\n";
        let res = decoder.decode(input.as_bytes(), &mut output);
        assert_eq!(DecoderResult::RecordBoundary, res);

        assert_eq!(1, output.num_records());

        let record = output.get_record(0);
        let fields: Vec<_> = record.iter_fields().collect();
        let expected: Vec<&[u8]> = vec![b"dddd", b"", b"ffffff"];
        assert_eq!(expected, fields);
    }
}
