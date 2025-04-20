use glaredb_core::arrays::datatype::DataType;
use glaredb_core::buffer::buffer_manager::AsRawBufferManager;
use glaredb_core::buffer::db_vec::DbVec;
use glaredb_error::{DbError, Result, ResultExt};

use super::encoding::PageDecoder;
use super::encoding::byte_stream_split::ByteStreamSplitDecoder;
use super::encoding::delta_length_byte_array::DeltaLengthByteArrayDecoder;
use super::encoding::dictionary::{Dictionary, DictionaryDecoder};
use super::encoding::rle_bit_packed::RleBitPackedDecoder;
use super::read_buffer::OwnedReadBuffer;
use super::value_reader::ValueReader;
use crate::basic::{self, Encoding};
use crate::column::encoding::delta_binary_packed::DeltaBinaryPackedDecoder;
use crate::column::encoding::delta_byte_array::DeltaByteArrayDecoder;
use crate::column::encoding::plain::PlainDecoder;
use crate::column::encoding::rle_bit_packed::RleBoolDecoder;
use crate::compression::Codec;
use crate::format;
use crate::page::{
    DataPageHeader,
    DataPageHeaderV2,
    DictionaryPageHeader,
    PageHeader,
    PageMetadata,
    PageType,
};
use crate::schema::types::ColumnDescriptor;
use crate::thrift::{TCompactSliceInputProtocol, TSerializable};
use crate::util::bit_util::num_required_bits;

// I don't remember writing this. I'm curious if it works.
#[derive(Debug)]
pub struct PageReader<V: ValueReader> {
    /// Column description.
    pub(crate) descr: ColumnDescriptor,
    /// Current offset into the chunk buffer.
    pub(crate) chunk_offset: usize,
    /// Column chunk buffer.
    pub(crate) chunk: DbVec<u8>,
    /// Decompressed page data for the current page.
    pub(crate) decompressed_page: OwnedReadBuffer,
    /// Decompression codec to use for this column.
    pub(crate) codec: Option<Box<dyn Codec>>,
    /// Current scan state.
    pub(crate) state: ScanState<V>,
}

/// State that gets updating during scanning.
#[derive(Debug)]
pub struct ScanState<V: ValueReader> {
    /// Number of values remaining for this page.
    ///
    /// Updated as we scan values from the page.
    pub remaining_page_values: usize,
    /// Definition levels decoder.
    pub definitions: Option<RleBitPackedDecoder>,
    /// Repetitions level decoder.
    pub repetitions: Option<RleBitPackedDecoder>,
    /// Decoder for this page.
    ///
    /// Should be Some after preparing a page.
    pub page_decoder: Option<PageDecoder<V>>,
    /// Dictionary for this column.
    ///
    /// Initially contains an empty array. Updated as we decode dictionary
    /// pages.
    pub dictionary: Dictionary<V>,
}

impl<V> PageReader<V>
where
    V: ValueReader,
{
    pub fn try_new(
        manager: &impl AsRawBufferManager,
        datatype: DataType,
        descr: ColumnDescriptor,
    ) -> Result<Self> {
        let chunk = DbVec::empty(manager);
        let decompressed_page = OwnedReadBuffer::new(DbVec::empty(manager));
        let dictionary = Dictionary::try_empty(manager, datatype)?;

        Ok(PageReader {
            descr,
            chunk_offset: 0,
            chunk,
            decompressed_page,
            codec: None, // TODO: ?
            state: ScanState {
                remaining_page_values: 0,
                definitions: None,
                repetitions: None,
                page_decoder: None,
                dictionary,
            },
        })
    }

    /// Prepares the next page by reading the next page from the chunk into this
    /// reader's decompressed page buffer.
    ///
    /// This will update the state with the new decoders, and the number of
    /// values available to read.
    pub fn prepare_next(&mut self) -> Result<()> {
        if self.chunk_offset >= self.chunk.capacity() {
            return Err(DbError::new("Reached end of page chunk, expected more")
                .with_field("chunk_offset", self.chunk_offset)
                .with_field("chunk_capacity", self.chunk.capacity()));
        }

        let header = self.read_header()?;
        let header = PageHeader::try_from_format(header, self.descr.physical_type())
            .context("failed to convert page header")?;

        match header.page_type {
            PageType::DataPage(page) => self.prepare_data_page(header.metadata, page)?,
            PageType::DataPageV2(page) => self.prepare_data_page_v2(header.metadata, page)?,
            PageType::Dictionary(page) => self.prepare_dictionary(header.metadata, page)?,
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
            def_dec.read(out)?;
        }

        if let Some(rep_dec) = &mut self.state.repetitions {
            let out = &mut repetitions[offset..(offset + count)];
            rep_dec.read(out)?;
        }

        Ok(())
    }

    fn prepare_dictionary(
        &mut self,
        metadata: PageMetadata,
        header: DictionaryPageHeader,
    ) -> Result<()> {
        // TODO: Duplicated with prepare v1.
        unsafe {
            self.decompressed_page
                .reset_and_resize(metadata.uncompressed_page_size as usize)?
        };
        let slice = self.chunk.as_slice();
        let src = slice
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
                // TODO: Check slice len

                // Page not compressed, just copy the data directly.
                let dest = unsafe { self.decompressed_page.remaining_as_slice_mut() };
                dest.copy_from_slice(src);
            }
        }

        // Dictionary specific stuff...
        let dict_size = header.num_values;
        let buffer = self.decompressed_page.take_remaining();
        self.state
            .dictionary
            .prepare_with_values(dict_size as usize, buffer)?;

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

        let slice = self.chunk.as_slice();
        let src = slice
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
                // TODO: Check slice len

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

        let mut get_level_decoder = |max: i16| -> Result<RleBitPackedDecoder> {
            // SAFETY: The `take_next` will error if we don't have at least 4
            // bytes in the buffer.
            let len = unsafe {
                self.decompressed_page
                    .take_next(4)?
                    .read_next_unchecked::<i32>()
            } as usize;

            let read_buffer = self.decompressed_page.take_next(len)?;
            let bit_width = num_required_bits(max as u64);

            Ok(RleBitPackedDecoder::new(read_buffer, bit_width))
        };

        if self.descr.max_rep_level > 0 {
            if header.rep_level_encoding != Encoding::RLE {
                return Err(DbError::new("RLE encoding required for repetition levels"));
            }
            self.state.repetitions = Some(get_level_decoder(self.descr.max_rep_level)?);
        } else {
            self.state.repetitions = None;
        }

        if self.descr.max_def_level > 0 {
            if header.def_level_encoding != Encoding::RLE {
                return Err(DbError::new("RLE encoding required for definition levels"));
            }
            self.state.definitions = Some(get_level_decoder(self.descr.max_def_level)?);
        } else {
            self.state.definitions = None;
        }

        // Prepare decoder.
        self.init_page_decoder(header.encoding)?;

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

        let is_compressed = self.codec.is_some();
        if !is_compressed {
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

            let uncompressed_len =
                (header.rep_levels_byte_len + header.def_levels_byte_len) as usize;

            // Copy in the uncompressed levels.
            let levels_dest = &mut dest[..uncompressed_len];
            let levels_src = Self::chunk_slice(&self.chunk, self.chunk_offset, uncompressed_len)?;
            levels_dest.copy_from_slice(levels_src);
            self.chunk_offset += uncompressed_len;

            // Now copy in the compressed page.
            let compressed_len = metadata.compressed_page_size as usize - uncompressed_len;
            let page_dest = &mut dest[uncompressed_len..];
            let page_src = Self::chunk_slice(&self.chunk, self.chunk_offset, compressed_len)?;
            self.chunk_offset += compressed_len;

            let codec = self.codec.as_ref().ok_or_else(|| {
                DbError::new(
                    "Page header indicates page is compressed, but we don't have a codec configured",
                )
            })?;

            if compressed_len > 0 {
                // Only try to decompress if there's something to decompress.
                //
                // It's valid for a page to contain zero bytes in the case of
                // all NULLs. Some codecs (snappy) don't like being handed zero
                // bytes as input.
                codec
                    .decompress(page_src, page_dest)
                    .context("failed to decompress page")?;
            }
        }

        // Init scan state for v2 data pages. We should only be reading from the
        // decompressed page now.
        //
        // Order: repetitions, definitions, page data
        //
        // Repetition and definition level lengths are encoded in the header.

        self.state.remaining_page_values = header.num_values as usize;

        let get_level_decoder =
            |page: &mut OwnedReadBuffer, max: i16, len: usize| -> Result<RleBitPackedDecoder> {
                let read_buffer = page.take_next(len)?;
                let bit_width = num_required_bits(max as u64);
                Ok(RleBitPackedDecoder::new(read_buffer, bit_width))
            };

        if self.descr.max_rep_level > 0 {
            // V2 only supports RLE for rep levels.
            self.state.repetitions = Some(get_level_decoder(
                &mut self.decompressed_page,
                self.descr.max_rep_level,
                header.rep_levels_byte_len as usize,
            )?);
        } else {
            // Turns out, you can have junk bytes in the repetition levels even
            // when the max level is zero.
            if header.rep_levels_byte_len > 0 {
                self.decompressed_page
                    .skip(header.rep_levels_byte_len as usize)?
            }
            self.state.repetitions = None;
        }

        if self.descr.max_def_level > 0 {
            // V2 only supports RLE for def levels.
            self.state.definitions = Some(get_level_decoder(
                &mut self.decompressed_page,
                self.descr.max_def_level,
                header.def_levels_byte_len as usize,
            )?);
        } else {
            // See repetitions...
            if header.def_levels_byte_len > 0 {
                self.decompressed_page
                    .skip(header.def_levels_byte_len as usize)?
            }
            self.state.definitions = None;
        }

        // Prepare decoder.
        self.init_page_decoder(header.encoding)?;

        Ok(())
    }

    /// Initializes the page decoder.
    ///
    /// Should only be called after we've processed a page header.
    fn init_page_decoder(&mut self, encoding: Encoding) -> Result<()> {
        // TODO: Document the `take_remaining` stuff a bit better.
        match encoding {
            Encoding::PLAIN => {
                let dec = PlainDecoder {
                    buffer: self.decompressed_page.take_remaining(),
                    value_reader: V::default(),
                };
                self.state.page_decoder = Some(PageDecoder::Plain(dec));

                Ok(())
            }
            Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                // Format: bit width is stored as one bytes, followed by RLE/BP
                // encoded values.
                let mut read_buffer = self.decompressed_page.take_remaining();
                let bit_width = unsafe { read_buffer.read_next_unchecked::<u8>() };

                let rle = RleBitPackedDecoder::new(read_buffer, bit_width);
                let dec = DictionaryDecoder::new(rle);
                self.state.page_decoder = Some(PageDecoder::Dictionary(dec));

                Ok(())
            }
            Encoding::RLE => {
                if self.descr.physical_type() != basic::Type::BOOLEAN {
                    return Err(DbError::new("RLE only valid for BOOL"));
                }

                let mut cursor = self.decompressed_page.take_remaining();

                // Format: rle-bit-packed-hybrid: <length> <encoded-data>
                //
                // Remove the length from the cursor, we don't need it. Cursor
                // is already the exact size.
                let _ = unsafe { cursor.read_next_unchecked::<u32>() };
                let dec = RleBoolDecoder::new(cursor);
                self.state.page_decoder = Some(PageDecoder::RleBool(dec));

                Ok(())
            }
            Encoding::DELTA_BINARY_PACKED => match self.descr.physical_type() {
                basic::Type::INT32 => {
                    // Creating the deocder will reader the header.
                    let read_buffer = self.decompressed_page.take_remaining();
                    let dec = DeltaBinaryPackedDecoder::<i32, V>::try_new(read_buffer)?;
                    self.state.page_decoder = Some(PageDecoder::DeltaBinaryPackedI32(dec));

                    Ok(())
                }
                basic::Type::INT64 => {
                    // See above
                    let read_buffer = self.decompressed_page.take_remaining();
                    let dec = DeltaBinaryPackedDecoder::<i64, V>::try_new(read_buffer)?;
                    self.state.page_decoder = Some(PageDecoder::DeltaBinaryPackedI64(dec));

                    Ok(())
                }
                other => Err(DbError::new(format!(
                    "Unsupported physical type for delta binary packed encoding: {other:?}"
                ))),
            },
            Encoding::DELTA_LENGTH_BYTE_ARRAY => {
                if self.descr.physical_type() != basic::Type::BYTE_ARRAY {
                    return Err(DbError::new(
                        "DELTA_LENGTH_BYTE_ARRAY only valid for BYTE_ARRAY",
                    ));
                }
                let cursor = self.decompressed_page.take_remaining();
                let verify_utf8 = true; // TODO
                let dec = DeltaLengthByteArrayDecoder::try_new(cursor, verify_utf8)?;
                self.state.page_decoder = Some(PageDecoder::DeltaLengthByteArray(dec));

                Ok(())
            }
            Encoding::DELTA_BYTE_ARRAY => {
                if !matches!(
                    self.descr.physical_type(),
                    basic::Type::BYTE_ARRAY | basic::Type::FIXED_LEN_BYTE_ARRAY,
                ) {
                    return Err(DbError::new(
                        "DELTA_BYTE_ARRAY only valid for BYTE_ARRAY or FIXED_LEN_BYTE_ARRAY",
                    ));
                }
                let cursor = self.decompressed_page.take_remaining();
                let verify_utf8 = true; // TODO
                let dec = DeltaByteArrayDecoder::try_new(cursor, verify_utf8)?;
                self.state.page_decoder = Some(PageDecoder::DeltaByteArray(dec));

                Ok(())
            }
            Encoding::BYTE_STREAM_SPLIT => {
                let cursor = self.decompressed_page.take_remaining();
                match self.descr.physical_type() {
                    basic::Type::INT32 | basic::Type::FLOAT => {
                        let dec = ByteStreamSplitDecoder::<4, _>::try_new(cursor)?;
                        self.state.page_decoder = Some(PageDecoder::ByteStreamSplit4(dec));
                        Ok(())
                    }
                    basic::Type::INT64 | basic::Type::DOUBLE => {
                        let dec = ByteStreamSplitDecoder::<8, _>::try_new(cursor)?;
                        self.state.page_decoder = Some(PageDecoder::ByteStreamSplit8(dec));
                        Ok(())
                    }
                    _ =>
                    // TODO: Fixed len byte array
                    {
                        Err(DbError::new(
                            "BYTE_STREAM_SPLIT only valid for INT32, INT64, FLOAT, DOUBLE",
                        ))
                    }
                }
            }
            other => Err(DbError::new("Unsupported encoding").with_field("encoding", other)),
        }
    }

    /// Gets a slice of the given size from the chunk starting at an offset.
    fn chunk_slice(chunk: &DbVec<u8>, offset: usize, size: usize) -> Result<&[u8]> {
        let slice = chunk.as_slice();
        let bs = slice
            .get(offset..(offset + size))
            .ok_or_else(|| DbError::new("chunk buffer not large enough to read from"))?;

        Ok(bs)
    }

    /// Reads the header at the current position.
    fn read_header(&mut self) -> Result<format::PageHeader> {
        let slice = self.chunk.as_slice();
        let buf = &slice[self.chunk_offset..];
        let mut prot = TCompactSliceInputProtocol::new(buf);
        let page_header = format::PageHeader::read_from_in_protocol(&mut prot)
            .context("failed to read page header")?;

        let bytes_read = buf.len() - prot.as_slice().len();
        self.chunk_offset += bytes_read;

        Ok(page_header)
    }
}
