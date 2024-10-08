pub mod generated;
pub mod packed;
pub mod util_types;

pub use prost;
use rayexec_error::Result;

/// Convert type to/from their protobuf representations.
///
/// This should be implemented for types that are stateless conversions.
pub trait ProtoConv: Sized {
    /// The type we're converting to/from.
    type ProtoType;

    fn to_proto(&self) -> Result<Self::ProtoType>;
    fn from_proto(proto: Self::ProtoType) -> Result<Self>;
}

// Not using `cfg(test)` here since that would make this not visible outside of
// this crate.
//
// See: <https://github.com/rust-lang/cargo/issues/8379>
pub mod testutil {
    use std::fmt::Debug;

    use crate::ProtoConv;

    /// Assert that a value roundtrips correctly through the conversion to and
    /// from a protobuf value.
    pub fn assert_proto_roundtrip<P: ProtoConv + PartialEq + Debug>(val: P) {
        let proto = val.to_proto().unwrap();
        let got = P::from_proto(proto).unwrap();

        assert_eq!(val, got);
    }
}

pub mod foreign_impls {
    use rayexec_error::{Result, ResultExt};
    use uuid::Uuid;

    use crate::ProtoConv;

    impl ProtoConv for Uuid {
        type ProtoType = crate::generated::foreign::Uuid;

        fn to_proto(&self) -> Result<Self::ProtoType> {
            Ok(Self::ProtoType {
                value: self.as_bytes().to_vec(),
            })
        }

        fn from_proto(proto: Self::ProtoType) -> Result<Self> {
            Self::from_slice(&proto.value).context("not a uuid slice")
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::testutil::assert_proto_roundtrip;

        #[test]
        fn uuid() {
            let v = Uuid::new_v4();
            assert_proto_roundtrip(v);
        }
    }
}
