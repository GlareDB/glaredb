use glaredb_error::{RayexecError, Result};
use rayexec_execution::arrays::array::Array;

use super::page_reader::PageReader;
use crate::column::encoding::{Definitions, PageDecoder};

#[derive(Debug)]
pub struct ColumnReader {
    pub(crate) page_reader: PageReader,
    pub(crate) definitions: Vec<i16>,
    pub(crate) repetitions: Vec<i16>,
}

impl ColumnReader {
    /// Reads `count` values into the output array.
    pub fn read(&mut self, output: &mut Array, count: usize) -> Result<()> {
        let mut offset = 0;
        let mut remaining = count;

        // Resize each buffer. The current values don't matter as they'll be
        // overwritten or not read at all.
        self.definitions.resize(count, 0);
        self.repetitions.resize(count, 0);

        while remaining > 0 {
            // Read next page.
            self.page_reader.prepare_next()?;
            if self.page_reader.state.remaining_page_values == 0 {
                // Page contains no values, continue to next one.
                continue;
            }

            let count = usize::min(remaining, self.page_reader.state.remaining_page_values);

            // Read in repetitions/definitions.
            self.page_reader.read_levels(
                &mut self.definitions,
                &mut self.repetitions,
                offset,
                count,
            )?;

            let definitions = if self.page_reader.state.definitions.is_some() {
                Definitions::HasDefinitions {
                    levels: &self.definitions,
                    max: self.page_reader.descr.max_def_level,
                }
            } else {
                Definitions::NoDefinitions
            };

            // Read the actual data.
            let decoder = match self.page_reader.state.page_decoder.as_mut() {
                Some(decoder) => decoder,
                None => return Err(RayexecError::new("Missing page decoder")),
            };
            let page_buf = &mut self.page_reader.state.page_buffer;
            match decoder {
                PageDecoder::Plain(dec) => {
                    dec.read_plain(page_buf, definitions, output, offset, count)?
                }
            }

            // Update page reader state.
            self.page_reader.state.remaining_page_values -= count;

            offset += count;
            remaining -= count;
        }

        Ok(())
    }
}
