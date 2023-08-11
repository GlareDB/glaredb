//! Types and conversions used throughout Metastore
//!
//! These types are generally one-to-one mappings of the types defined in the
//! protobuf definitions, except without some optionals. Conversion from protobuf
//! to the types defined in this module should ensure the values validity.

// pub mod arrow;
pub mod catalog;
pub mod options;
pub mod service;
pub mod storage;


