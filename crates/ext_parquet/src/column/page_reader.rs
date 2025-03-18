use std::sync::Arc;

use glaredb_error::{DbError, Result, ResultExt};
use glaredb_execution::buffer::typed::ByteBuffer;

use super::encoding::rle_bp::RleBpDecoder;
use super::encoding::PageDecoder;
use super::read_buffer::{OwnedReadBuffer, ReadBuffer};
use crate::basic::Encoding;
use crate::compression::Codec;
use crate::format;
use crate::page::{DataPageHeader, DataPageHeaderV2, PageHeader, PageMetadata, PageType};
use crate::schema::types::ColumnDescriptor;
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};
use crate::util::bit_util::num_required_bits;

#[derive(Debug)]
pub struct PageReader {
    /// Column description.
    pub(crate) descr: Arc<ColumnDescriptor>,
    /// Current offset into the chunk buffer.
    pub(crate) chunk_offset: usize,
    /// Column chunk buffer.
    pub(crate) chunk: ByteBuffer,
    /// Decompressed page data for the current page.
    pub(crate) decompressed_page: OwnedReadBuffer,
    /// Decompression codec to use for this column.
    pub(crate) codec: Option<Box<dyn Codec>>,
    /// Current scan state.
    pub(crate) state: ScanState,
}

/// State that gets updating during scanning.
#[derive(Debug)]
pub struct ScanState {
    /// Number of values remaining for this page.
    ///
    /// Updated as we scan values from the page.
    pub remaining_page_values: usize,
    /// Definition levels decoder.
    pub definitions: Option<RleBpDecoder>,
    /// Repetitions level decoder.
    pub repetitions: Option<RleBpDecoder>,
    /// Decoder for this page.
    ///
    /// Should be Some after preparing a page.
    pub page_decoder: Option<PageDecoder>,
    /// Buffer for the page. Should be passed to the decoder.
    pub page_buffer: ReadBuffer,
}

impl PageReader {
    /// Prepares the next page by reading the next page from the chunk into this
    /// reader's decompressed page buffer.
    ///
    /// This will update the state with the new decoders, and the number of
    /// values available to read.
    pub fn prepare_next(&mut self) -> Result<()> {
        if self.chunk_offset >= self.chunk.capacity() {
            return Err(DbError::new("reached end of chunk"));
        }

        let header = self.read_header()?;
        let header = PageHeader::try_from_format(header, self.descr.physical_type())
            .context("failed to convert page header")?;

        match header.page_type {
            PageType::DataPage(page) => self.prepare_data_page(header.metadata, page)?,
            PageType::DataPageV2(page) => self.prepare_data_page_v2(header.metadata, page)?,
            PageType::Dictionary(_) => unimplemented!(),
        }

        Ok(())
    }

    /// Reads definition and repetition levels into the provided slices.
    ///
    /// Both slices mut be able to accomadate the `offset` and `count` number of
    /// elements.
    pub fn read_levels(
        &mut self,
        definitions: &mut [i16],
        repetitions: &mut [i16],
        offset: usize,
        count: usize,
    ) -> Result<()> {
        if let Some(def_dec) = &mut self.state.definitions {
            let out = &mut definitions[offset..(offset + count)];
            def_dec.get_batch(out)?;
        }

        if let Some(rep_dec) = &mut self.state.repetitions {
            let out = &mut repetitions[offset..(offset + count)];
            rep_dec.get_batch(out)?;
        }

        Ok(())
    }

    fn prepare_data_page(&mut self, metadata: PageMetadata, header: DataPageHeader) -> Result<()> {
        // Ensure our read buffer can fit the entire decompressed page.
        //
        // SAFETY: The only other read buffers we get for the decompressed page
        // are for the repetition and definition levels. New buffers will be
        // created prior to attempting to read the levels.
        unsafe {
            self.decompressed_page
                .reset_and_resize(metadata.uncompressed_page_size as usize)?
        };

        let src = &self
            .chunk
            .as_slice()
            .get(self.chunk_offset..(self.chunk_offset + metadata.compressed_page_size as usize))
            .ok_or_else(|| DbError::new("chunk buffer not large enough to read from"))?;

        self.chunk_offset += metadata.compressed_page_size as usize;

        match self.codec.as_ref() {
            Some(codec) => {
                // Page is compressed, decompress into our read buffer.
                //
                // SAFETY: No concurrent reads.
                let dest = unsafe { self.decompressed_page.remaining_as_slice_mut() };
                codec
                    .decompress(src, dest)
                    .context("failed to decompress page")?;
            }
            None => {
                // Page not compressed, just copy the data directly.
                let dest = unsafe { self.decompressed_page.remaining_as_slice_mut() };
                dest.copy_from_slice(src);
            }
        }

        // Init scan state for v1 data pages. We should only be reading from the
        // decompressed page now.
        //
        // Order: repetitions, definitions, page data
        //
        // Repetition and definition level lengths are encoded inline as i32.

        self.state.remaining_page_values = header.num_values as usize;

        let mut get_level_decoder = |max: i16| -> Result<RleBpDecoder> {
            // SAFETY: The `take_next` will error if we don't have at least 4
            // bytes in the buffer.
            let len = unsafe {
                self.decompressed_page
                    .take_next(4)?
                    .read_next_unchecked::<i32>()
            } as usize;

            let read_buffer = self.decompressed_page.take_next(len)?;
            let bit_width = num_required_bits(max as u64);

            Ok(RleBpDecoder::new(read_buffer, bit_width))
        };

        if self.descr.max_rep_level > 0 {
            if header.rep_level_encoding != Encoding::RLE {
                return Err(DbError::new("RLE encoding required for repetition levels"));
            }

            self.state.repetitions = Some(get_level_decoder(self.descr.max_rep_level)?);
        }

        if self.descr.max_def_level > 0 {
            if header.rep_level_encoding != Encoding::RLE {
                return Err(DbError::new("RLE encoding required for definition levels"));
            }

            self.state.definitions = Some(get_level_decoder(self.descr.max_rep_level)?);
        }

        Ok(())
    }

    fn prepare_data_page_v2(
        &mut self,
        metadata: PageMetadata,
        header: DataPageHeaderV2,
    ) -> Result<()> {
        // SAFETY: See data page v1.
        unsafe {
            self.decompressed_page
                .reset_and_resize(metadata.uncompressed_page_size as usize)?
        };

        // SAFETY: No concurrent reads.
        let dest = unsafe { self.decompressed_page.remaining_as_slice_mut() };

        if !header.is_compressed {
            // Can just read as-is.
            let src = Self::chunk_slice(
                &self.chunk,
                self.chunk_offset,
                metadata.compressed_page_size as usize,
            )?;
            dest.copy_from_slice(src);
            self.chunk_offset += src.len();
        } else {
            // Otherwise need to compute the proper compressed size since data page
            // v2 leaves rep and def levels uncompressed.

            let uncompressed_count =
                (header.rep_levels_byte_len + header.def_levels_byte_len) as usize;

            // Copy in the uncompressed levels.
            let levels_dest = &mut dest[..uncompressed_count];
            let levels_src = Self::chunk_slice(&self.chunk, self.chunk_offset, uncompressed_count)?;
            levels_dest.copy_from_slice(levels_src);
            self.chunk_offset += uncompressed_count;

            // Now copy in the compressed page.
            let compressed_count = metadata.compressed_page_size as usize - uncompressed_count;
            let page_dest = &mut dest[uncompressed_count..];
            let page_src = Self::chunk_slice(&self.chunk, self.chunk_offset, compressed_count)?;
            self.chunk_offset += compressed_count;

            let codec = self.codec.as_ref().ok_or_else(|| {
                DbError::new(
                "Page header indicates page is compressed, but we don't have a codec configured",
            )
            })?;

            codec
                .decompress(page_src, page_dest)
                .context("failed to decompress page")?;
        }

        // Init scan state for v2 data pages. We should only be reading from the
        // decompressed page now.
        //
        // Order: repetitions, definitions, page data
        //
        // Repetition and definition level lengths are encoded in the header.

        self.state.remaining_page_values = header.num_values as usize;

        let mut get_level_decoder = |max: i16, len: usize| -> Result<RleBpDecoder> {
            let read_buffer = self.decompressed_page.take_next(len)?;
            let bit_width = num_required_bits(max as u64);
            Ok(RleBpDecoder::new(read_buffer, bit_width))
        };

        if self.descr.max_rep_level > 0 {
            // V2 only supports RLE for rep levels.
            self.state.repetitions = Some(get_level_decoder(
                self.descr.max_rep_level,
                header.rep_levels_byte_len as usize,
            )?);
        }

        if self.descr.max_def_level > 0 {
            // V2 only supports RLE for def levels.
            self.state.definitions = Some(get_level_decoder(
                self.descr.max_rep_level,
                header.def_levels_byte_len as usize,
            )?);
        }

        Ok(())
    }

    /// Initializes the page decoder.
    ///
    /// Should only be called after we've processed a page header.
    #[allow(unused)]
    fn init_page_decoder(&mut self, encoding: Encoding) -> Result<()> {
        match encoding {
            Encoding::PLAIN => {
                // Taking remaining...
                unimplemented!()
            }
            other => Err(DbError::new("Unsupported encoding").with_field("encoding", other)),
        }
    }

    /// Gets a slice of the given size from the chunk starting at an offset.
    fn chunk_slice(chunk: &ByteBuffer, offset: usize, size: usize) -> Result<&[u8]> {
        let bs = &chunk
            .as_slice()
            .get(offset..(offset + size))
            .ok_or_else(|| DbError::new("chunk buffer not large enough to read from"))?;

        Ok(bs)
    }

    /// Reads the header at the current position.
    fn read_header(&mut self) -> Result<format::PageHeader> {
        let buf = &self.chunk.as_slice()[self.chunk_offset..];
        let mut prot = TCompactSliceInputProtocol::new(buf);
        let page_header = format::PageHeader::read_from_in_protocol(&mut prot)
            .context("failed to read page header")?;

        let bytes_read = buf.len() - prot.as_slice().len();
        self.chunk_offset += bytes_read;

        Ok(page_header)
    }
}
