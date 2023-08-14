//! Types and conversions used throughout Metastore
//!
//! These types are generally one-to-one mappings of the types defined in the
//! protobuf definitions, except without some optionals. Conversion from protobuf
//! to the types defined in this module should ensure the values validity.

use crate::errors::ProtoConvError;

pub mod catalog;
pub mod options;
pub mod service;
pub mod storage;

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = ProtoConvError>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    fn optional(self) -> Result<Option<T>, ProtoConvError>;

    /// Converts an optional protobuf field to a different type, returning an
    /// error if None.
    fn required(self, field: impl Into<String>) -> Result<T, ProtoConvError>;
}

impl<T, U> FromOptionalField<U> for Option<T>
where
    T: TryInto<U, Error = ProtoConvError>,
{
    fn optional(self) -> Result<Option<U>, ProtoConvError> {
        self.map(|t| t.try_into()).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, ProtoConvError> {
        match self {
            None => Err(ProtoConvError::RequiredField(field.into())),
            Some(t) => t.try_into(),
        }
    }
}
