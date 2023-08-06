//! Centralized protobuf and type definitions.
//!
//! All protobufs and types shared across service boundaries should be placed in
//! this crate. This crate should be able to imported by any other crate in the
//! project. There should be a minimal amount of logic in this crate.

pub mod metastore;

/// Generated code.
pub mod gen {
    pub mod rpcsrv {
        pub mod service {
            tonic::include_proto!("rpcsrv.service");
        }
    }

    pub mod metastore {
        pub mod arrow {
            tonic::include_proto!("metastore.arrow");
        }

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
