use rayexec_error::{OptionExt, RayexecError, Result, ResultExt};
use rayexec_execution::arrays::array::buffer_manager::BufferManager;
use rayexec_execution::arrays::array::raw::ByteBuffer;

use super::column_data::ColumnData;
use super::page::Page;
use super::read_buffer::ReadBuffer;
use crate::compression::Codec;
use crate::format::{PageHeader, PageType};
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};

#[derive(Debug)]
pub struct PageReader<B: BufferManager> {
    /// Current offset into the chunk buffer.
    pub(crate) chunk_offset: usize,
    /// Column chunk buffer.
    pub(crate) chunk: ByteBuffer<B>,
    /// Decompressed page data for the current page.
    pub(crate) decompressed_page: ReadBuffer<B>,
    /// Decompression codec to use for this column.
    pub(crate) codec: Option<Box<dyn Codec>>,
}

#[derive(Debug)]
pub struct ScanState {
    /// Number of values remaining for this page.
    ///
    /// Updated as we scan values from the page.
    pub num_values: usize,
}

impl<B> PageReader<B>
where
    B: BufferManager,
{
    /// Reads the next page from the chunk.
    ///
    /// Returns None if there's no more pages to read in the chunk.
    pub fn read_next(&mut self) -> Result<Option<Page>> {
        if self.chunk_offset >= self.chunk.capacity() {
            return Ok(None);
        }

        let header = self.read_header()?;

        match header.type_ {
            PageType::DATA_PAGE => {
                self.read_data_page(&header)?;
                unimplemented!()
            }
            PageType::DATA_PAGE_V2 => {
                self.read_data_page_v2(&header)?;
                unimplemented!()
            }
            PageType::DICTIONARY_PAGE => {
                //
                unimplemented!()
            }
            _ => unimplemented!(),
        }

        unimplemented!()
    }

    fn read_data_page(&mut self, header: &PageHeader) -> Result<()> {
        // Ensure our read buffer can fit the entire decompressed page.
        self.decompressed_page
            .reset_for_new_page(header.uncompressed_page_size as usize)?;

        let src = &self
            .chunk
            .as_slice()
            .get(self.chunk_offset..(self.chunk_offset + header.compressed_page_size as usize))
            .ok_or_else(|| RayexecError::new("chunk buffer not large enough to read from"))?;

        self.chunk_offset += header.compressed_page_size as usize;

        match self.codec.as_ref() {
            Some(codec) => {
                // Page is compressed, decompress into our read buffer.
                let dest = self.decompressed_page.as_slice_mut();
                codec
                    .decompress(src, dest)
                    .context("failed to decompress page")?;
            }
            None => {
                // Page not compressed, just copy the data directly.
                let dest = self.decompressed_page.as_slice_mut();
                dest.copy_from_slice(src);
            }
        }

        Ok(())
    }

    fn read_data_page_v2(&mut self, header: &PageHeader) -> Result<()> {
        self.decompressed_page
            .reset_for_new_page(header.uncompressed_page_size as usize)?;

        let header_v2 = header
            .data_page_header_v2
            .as_ref()
            .ok_or_else(|| RayexecError::new("expected v2 header when reading v2 data page"))?;

        // Missing 'is_compressed' should be interpreted as true according to
        // spec.
        let is_compressed = header_v2.is_compressed.unwrap_or(true);

        let dest = self.decompressed_page.as_slice_mut();

        if !is_compressed {
            // Can just read as-is.
            let src = Self::chunk_slice(
                &self.chunk,
                self.chunk_offset,
                header.compressed_page_size as usize,
            )?;
            dest.copy_from_slice(src);
            self.chunk_offset += src.len();

            return Ok(());
        }

        // Otherwise need to compute the proper compressed size since data page
        // v2 leaves rep and def levels uncompressed.

        let uncompressed_count = (header_v2.repetition_levels_byte_length
            + header_v2.definition_levels_byte_length) as usize;

        // Copy in the uncompressed levels.
        let levels_dest = &mut dest[..uncompressed_count];
        let levels_src = Self::chunk_slice(&self.chunk, self.chunk_offset, uncompressed_count)?;
        levels_dest.copy_from_slice(levels_src);
        self.chunk_offset += uncompressed_count;

        // Now copy in the compressed page.
        let compressed_count = header.compressed_page_size as usize - uncompressed_count;
        let page_dest = &mut dest[uncompressed_count..];
        let page_src = Self::chunk_slice(&self.chunk, self.chunk_offset, compressed_count)?;
        self.chunk_offset += compressed_count;

        let codec = self.codec.as_ref().ok_or_else(|| {
            RayexecError::new(
                "Page header indicates page is compressed, but we don't have a codec configured",
            )
        })?;

        codec
            .decompress(page_src, page_dest)
            .context("failed to decompress page")?;

        Ok(())
    }

    /// Gets a slice of the given size from the chunk starting at an offset.
    fn chunk_slice(chunk: &ByteBuffer<B>, offset: usize, size: usize) -> Result<&[u8]> {
        let bs = &chunk
            .as_slice()
            .get(offset..(offset + size))
            .ok_or_else(|| RayexecError::new("chunk buffer not large enough to read from"))?;

        Ok(bs)
    }

    /// Reads the header at the current position.
    fn read_header(&mut self) -> Result<PageHeader> {
        let buf = &self.chunk.as_slice()[self.chunk_offset..];
        let mut prot = TCompactSliceInputProtocol::new(buf);
        let page_header =
            PageHeader::read_from_in_protocol(&mut prot).context("failed to read page header")?;

        let bytes_read = buf.len() - prot.as_slice().len();
        self.chunk_offset += bytes_read;

        Ok(page_header)
    }
}
