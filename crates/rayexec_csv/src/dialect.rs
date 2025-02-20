use std::fmt;

use crate::decoder::{ByteRecords, CsvDecoder};

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct DialectOptions {
    /// Delimiter character.
    pub delimiter: u8,
    /// Quote character.
    pub quote: u8,
}

impl Default for DialectOptions {
    fn default() -> Self {
        DialectOptions {
            delimiter: b',',
            quote: b'"',
        }
    }
}

impl fmt::Debug for DialectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DialectOptions")
            .field("deliminator", &(self.delimiter as char))
            .field("quote", &(self.quote as char))
            .finish()
    }
}

impl DialectOptions {
    /// Try to infer which csv options to use based on some number of records
    /// from a csv source.
    ///
    /// The provided byte records used is to buffer the output between testing
    /// the dialects. The records will be cleared prior to returning.
    pub fn infer_from_sample(sample_bytes: &[u8], output: &mut ByteRecords) -> Option<Self> {
        // Best dialect chosen so far alongside number of fields decoded.
        let mut best: (Option<Self>, usize) = (None, 0);

        for dialect in Self::dialects() {
            output.clear_all();

            // To be considered the best dialect:
            //
            // - Should decode at least 2 records.
            // - Should parse at least 2 fields for a record.
            // - Should have decoded more number of fields than previous best.
            // - All decoded records have the same number of fields.

            let mut decoder = CsvDecoder::new(*dialect);
            let _ = decoder.decode(sample_bytes, output);

            if output.num_records() < 2 {
                continue;
            }

            let rec = output.get_record(0);
            let num_fields = rec.num_fields();

            // Parsing a single field is trivial.
            if num_fields < 2 {
                continue;
            }

            // If we parse fewer fields, likely not what we want.
            if num_fields <= best.1 {
                continue;
            }

            // Make sure everything in our sample has the same number of
            // fields.
            let check_num_fields = || -> bool {
                for record in output.iter_records() {
                    if record.num_fields() != num_fields {
                        return false;
                    }
                }
                true
            };
            if !check_num_fields() {
                continue;
            }

            // New best dialect.
            best = (Some(*dialect), num_fields);
        }

        output.clear_all();

        best.0
    }

    pub(crate) fn csv_core_reader(&self) -> csv_core::Reader {
        csv_core::ReaderBuilder::new()
            .delimiter(self.delimiter)
            .quote(self.quote)
            .build()
    }

    /// Dialects used when attempting to infer options for a csv file.
    ///
    /// These are order with preferred options first. For example, if we infer
    /// the dialects from a file containing only commas, we want to pick the
    /// dialect with the more standard quote character ('"').
    const fn dialects() -> &'static [Self] {
        &[
            DialectOptions {
                delimiter: b',',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b'|',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b';',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b'\t',
                quote: b'"',
            },
            DialectOptions {
                delimiter: b',',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b'|',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b';',
                quote: b'\'',
            },
            DialectOptions {
                delimiter: b'\t',
                quote: b'\'',
            },
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn infer_none_too_few_records() {
        let mut output = ByteRecords::with_buffer_capacity(64);

        let input = b"a,b,c\nd,e";
        let dialect = DialectOptions::infer_from_sample(input, &mut output);

        assert_eq!(None, dialect);
    }

    #[test]
    fn infer_none_inconsistent_deliminators() {
        let mut output = ByteRecords::with_buffer_capacity(64);

        let input = b"a,b,c\nd|e|f\ng,h";
        let dialect = DialectOptions::infer_from_sample(input, &mut output);

        assert_eq!(None, dialect);
    }

    #[test]
    fn infer_default() {
        let mut output = ByteRecords::with_buffer_capacity(64);

        let input = b"a,b,c\nd,e,f\ng,h";
        let dialect = DialectOptions::infer_from_sample(input, &mut output);

        assert_eq!(
            Some(DialectOptions {
                delimiter: b',',
                quote: b'"',
            }),
            dialect
        );
    }

    #[test]
    fn infer_different_deliminator() {
        let mut output = ByteRecords::with_buffer_capacity(64);

        let input = b"a|b|c\nd|e,e,e,e|f\ng|h";
        let dialect = DialectOptions::infer_from_sample(input, &mut output);

        assert_eq!(
            Some(DialectOptions {
                delimiter: b'|',
                quote: b'"',
            }),
            dialect
        );
    }
}
