use std::io::Write as _;

use csv::ByteRecord;
use rayexec_bullet::batch::Batch;
use rayexec_bullet::field::Schema;
use rayexec_bullet::format::{FormatOptions, Formatter};
use rayexec_error::{Result, ResultExt};

use crate::reader::DialectOptions;

#[derive(Debug)]
pub struct CsvEncoder {
    /// Schema of the batches we're writing. Used to write the header out.
    schema: Schema,

    /// If we've already written the header.
    did_write_header: bool,

    /// Dialect of csv we're writing.
    dialect: DialectOptions,

    /// Buffer used for formatting the batch.
    format_buf: Vec<u8>,

    /// Buffer for current record.
    record: ByteRecord,
}

impl CsvEncoder {
    pub fn new(schema: Schema, dialect: DialectOptions) -> Self {
        let record = ByteRecord::with_capacity(1024, schema.fields.len());
        CsvEncoder {
            schema,
            dialect,
            did_write_header: false,
            format_buf: Vec::with_capacity(1024),
            record,
        }
    }

    pub fn encode(&mut self, batch: &Batch, output_buf: &mut Vec<u8>) -> Result<()> {
        const FORMATTER: Formatter = Formatter::new(FormatOptions::new());

        let mut csv_writer = csv::WriterBuilder::new()
            .delimiter(self.dialect.delimiter)
            .quote(self.dialect.quote)
            .from_writer(output_buf);

        if !self.did_write_header {
            for col_name in self.schema.fields.iter().map(|f| &f.name) {
                self.record.push_field(col_name.as_bytes());
            }
            csv_writer
                .write_record(&self.record)
                .context("failed to write header")?;

            self.did_write_header = true;
        }

        for row in 0..batch.num_rows() {
            self.record.clear();

            for col in batch.columns() {
                let scalar = FORMATTER
                    .format_array_value(col, row)
                    .expect("row to exist");
                self.format_buf.clear();
                write!(&mut self.format_buf, "{}", scalar).expect("write to succeed");

                self.record.push_field(&self.format_buf);
            }

            csv_writer
                .write_record(&self.record)
                .context("failed to write record")?;
        }

        csv_writer.flush().context("failed to flush")?;

        Ok(())
    }
}
