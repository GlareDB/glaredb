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

//! Low level APIs for reading raw parquet data.
//!
//! Provides access to file and row group readers and writers, record API, metadata, etc.

pub mod footer;
pub mod metadata;
pub mod page_encoding_stats;
pub mod page_index;
pub mod properties;
pub mod statistics;

/// The length of the parquet footer in bytes
pub const FOOTER_SIZE: usize = 8;

/// Magic value for parquet files.
pub const PARQUET_MAGIC: &[u8; 4] = b"PAR1";

/// Magic value for encrypted parquet files.
pub const PARQUET_MAGIC_ENC: &[u8; 4] = b"PARE";
