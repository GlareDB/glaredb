// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! This crate contains the official Native Rust implementation of
//! [Apache Parquet](https://parquet.apache.org/), part of
//! the [Apache Arrow](https://arrow.apache.org/) project.
//! The crate provides a number of APIs to read and write Parquet files,
//! covering a range of use cases.
//!
//! Please see the [parquet crates.io](https://crates.io/crates/parquet)
//! page for feature flags and tips to improve performance.
//!
//! # Format Overview
//!
//! Parquet is a columnar format, which means that unlike row formats like [CSV], values are
//! iterated along columns instead of rows. Parquet is similar in spirit to [Arrow], but
//! focuses on storage efficiency whereas Arrow prioritizes compute efficiency.
//!
//! Parquet files are partitioned for scalability. Each file contains metadata,
//! along with zero or more "row groups", each row group containing one or
//! more columns. The APIs in this crate reflect this structure.
//!
//! Data in Parquet files is strongly typed and differentiates between logical
//! and physical types (see [`schema`]). In addition, Parquet files may contain
//! other metadata, such as statistics, which can be used to optimize reading
//! (see [`file::metadata`]).
//! For more details about the Parquet format itself, see the [Parquet spec]
//!
//! [Parquet spec]: https://github.com/apache/parquet-format/blob/master/README.md#file-format
//!
//! # APIs
//!
//! This crate exposes a number of APIs for different use-cases.
//!
//! ## Metadata and Schema
//!
//! The [`schema`] module provides APIs to work with Parquet schemas. The
//! [`file::metadata`] module provides APIs to work with Parquet metadata.
//!
//! ## Read/Write Parquet
//!
//! Workloads needing finer-grained control, or avoid a dependence on arrow,
//! can use the lower-level APIs in [`mod@file`]. These APIs expose the underlying parquet
//! data model, and therefore require knowledge of the underlying parquet format,
//! including the details of [Dremel] record shredding and [Logical Types]. Most workloads
//! should prefer the arrow interfaces.
//!
//! [CSV]: https://en.wikipedia.org/wiki/Comma-separated_values
//! [Dremel]: https://research.google/pubs/pub36632/
//! [Logical Types]: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md

pub mod basic;
pub mod errors;

/// Automatically generated code for reading parquet thrift definition.
// see parquet/CONTRIBUTING.md for instructions on regenerating
#[allow(clippy::derivable_impls, clippy::match_single_binding, clippy::doc_lazy_continuation)]
// Don't try and format auto generated code
#[rustfmt::skip]
pub mod format;

#[macro_use]
pub mod data_type;

pub use self::encodings::{decoding, encoding};

#[macro_use]
mod util;

pub mod bloom_filter;
pub mod column;
pub mod column_reader; // TODO: Rename
mod compression;
mod encodings;
pub mod file;
pub mod reader;
pub mod schema;

pub mod thrift;

#[cfg(test)]
pub mod testutil;
