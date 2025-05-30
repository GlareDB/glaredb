pub mod byte_stream_split;
pub mod delta_binary_packed;
pub mod delta_byte_array;
pub mod delta_length_byte_array;
pub mod dictionary;
pub mod plain;
pub mod rle_bit_packed;

use byte_stream_split::ByteStreamSplitDecoder;
use delta_binary_packed::DeltaBinaryPackedDecoder;
use delta_byte_array::DeltaByteArrayDecoder;
use delta_length_byte_array::DeltaLengthByteArrayDecoder;
use dictionary::DictionaryDecoder;
use plain::PlainDecoder;
use rle_bit_packed::RleBoolDecoder;

use super::value_reader::ValueReader;

#[derive(Debug)]
pub enum Definitions<'a> {
    /// This column has definitions.
    HasDefinitions {
        /// Definitions levels.
        ///
        /// When passing to a page decoder, this should be the exact number of
        /// values we expect to read from that decoder.
        levels: &'a [i16],
        /// Max definition level. Used to determine nullability.
        max: i16,
    },
    /// This column does not have any definitions.
    NoDefinitions,
}

#[derive(Debug)]
pub enum PageDecoder<V: ValueReader> {
    Plain(PlainDecoder<V>),
    Dictionary(DictionaryDecoder<V>),
    DeltaBinaryPackedI32(DeltaBinaryPackedDecoder<i32, V>),
    DeltaBinaryPackedI64(DeltaBinaryPackedDecoder<i64, V>),
    DeltaLengthByteArray(DeltaLengthByteArrayDecoder),
    DeltaByteArray(DeltaByteArrayDecoder),
    RleBool(RleBoolDecoder),
    ByteStreamSplit4(ByteStreamSplitDecoder<4, V>),
    ByteStreamSplit8(ByteStreamSplitDecoder<8, V>),
}
