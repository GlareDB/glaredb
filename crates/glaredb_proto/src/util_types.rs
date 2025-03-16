use prost::Message;

#[derive(Clone, PartialEq, Message)]
pub struct OptionalString {
    #[prost(message, tag = "1")]
    pub value: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packed::{PackedDecoder, PackedEncoder};

    #[test]
    fn packed_optional_string_none() {
        let s = OptionalString { value: None };
        let mut buf = Vec::new();
        PackedEncoder::new(&mut buf).encode_next(&s).unwrap();

        let got: OptionalString = PackedDecoder::new(&buf).decode_next().unwrap();
        assert_eq!(None, got.value);
    }

    #[test]
    fn packed_optional_string_some() {
        let s = OptionalString {
            value: Some("mario".to_string()),
        };
        let mut buf = Vec::new();
        PackedEncoder::new(&mut buf).encode_next(&s).unwrap();

        let got: OptionalString = PackedDecoder::new(&buf).decode_next().unwrap();
        assert_eq!(Some("mario".to_string()), got.value);
    }
}
