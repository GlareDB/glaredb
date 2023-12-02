//! Centralized protobuf and type definitions.
//!
//! All protobufs and types shared across service boundaries should be placed in
//! this crate. This crate should be able to imported by any other crate in the
//! project. There should be a minimal amount of logic in this crate.
#![allow(non_snake_case)]

pub mod common;
pub mod metastore;
pub mod sqlexec;

pub mod rpcsrv;
pub mod export {
    pub use prost;
}
pub use errors::ProtoConvError;

pub mod errors {
    /// Errors related to converting to/from protobuf types.
    #[derive(thiserror::Error, Debug)]
    pub enum ProtoConvError {
        #[error("Field required: {0}")]
        RequiredField(String),

        #[error("Unknown enum variant for '{0}': {1}")]
        UnknownEnumVariant(&'static str, i32),

        #[error("Received zero-value enum variant for '{0}'")]
        ZeroValueEnumVariant(&'static str),

        #[error("Unsupported serialization: {0}")]
        UnsupportedSerialization(&'static str),

        #[error("Invalid table reference: {0:?}.{1:?}.{2:?}")]
        InvalidTableReference(String, String, String),

        #[error("Parse Error: {0}")]
        ParseError(String),

        #[error(transparent)]
        TimestampError(#[from] prost_types::TimestampError),

        #[error(transparent)]
        Uuid(#[from] uuid::Error),

        #[error(transparent)]
        TryFromIntError(#[from] std::num::TryFromIntError),

        #[error(transparent)]
        DecodeError(#[from] prost::DecodeError),

        #[error(transparent)]
        DfLogicalFromProto(#[from] datafusion_proto::logical_plan::from_proto::Error),

        #[error(transparent)]
        DfLogicalToProto(#[from] datafusion_proto::logical_plan::to_proto::Error),

        #[error(transparent)]
        DataFusionError(#[from] datafusion::common::DataFusionError),
    }

    impl From<ProtoConvError> for tonic::Status {
        fn from(value: ProtoConvError) -> Self {
            Self::from_error(Box::new(value))
        }
    }

    impl From<ProtoConvError> for datafusion::error::DataFusionError {
        fn from(value: ProtoConvError) -> Self {
            Self::External(Box::new(value))
        }
    }

    impl From<std::convert::Infallible> for ProtoConvError {
        fn from(value: std::convert::Infallible) -> Self {
            unreachable!()
        }
    }
}

/// Generated code.
pub mod gen {
    pub mod datafusion {
        pub use datafusion_proto::generated::datafusion::*;
    }

    pub mod common {
        pub mod arrow {
            tonic::include_proto!("common.arrow");
        }
    }

    pub mod rpcsrv {
        pub mod service {
            tonic::include_proto!("rpcsrv.service");
        }

        pub mod common {
            tonic::include_proto!("rpcsrv.common");
        }

        pub mod simple {
            tonic::include_proto!("rpcsrv.simple");
        }
    }

    pub mod metastore {
        pub mod catalog {
            tonic::include_proto!("metastore.catalog");
        }

        pub mod service {
            tonic::include_proto!("metastore.service");
        }

        pub mod storage {
            tonic::include_proto!("metastore.storage");
        }

        pub mod options {
            tonic::include_proto!("metastore.options");
        }
    }
}

/// An extension trait that adds the methods `optional` and `required` to any
/// Option containing a type implementing `TryInto<U, Error = ProtoConvError>`
pub trait FromOptionalField<T> {
    /// Converts an optional protobuf field to an option of a different type
    fn optional(self) -> Result<Option<T>, ProtoConvError>;

    /// Converts an optional protobuf field to a different type, returning an
    /// error if None.
    fn required(self, field: impl Into<String>) -> Result<T, ProtoConvError>;
}

impl<T, U, E> FromOptionalField<U> for Option<T>
where
    E: Into<ProtoConvError>,
    T: TryInto<U, Error = E>,
{
    fn optional(self) -> Result<Option<U>, ProtoConvError> {
        self.map(|t| t.try_into().map_err(|e| e.into())).transpose()
    }

    fn required(self, field: impl Into<String>) -> Result<U, ProtoConvError> {
        match self {
            None => Err(ProtoConvError::RequiredField(field.into())),
            Some(t) => t.try_into().map_err(|e| e.into()),
        }
    }
}
