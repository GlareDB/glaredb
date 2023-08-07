//! Centralized protobuf and type definitions.
//!
//! All protobufs and types shared across service boundaries should be placed in
//! this crate. This crate should be able to imported by any other crate in the
//! project. There should be a minimal amount of logic in this crate.

pub mod metastore;
