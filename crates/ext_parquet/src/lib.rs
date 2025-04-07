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

pub mod basic;
#[allow(clippy::derivable_impls, clippy::match_single_binding, clippy::doc_lazy_continuation)]
#[rustfmt::skip]
pub mod format;

#[macro_use]
pub mod data_type;

#[macro_use]
mod util;

pub mod bloom_filter;
pub mod column;
pub mod page;
pub mod row_groups_reader;
pub mod schema;

pub mod thrift;

pub mod extension;
pub mod functions;
pub mod metadata;

mod compression;
mod encodings;

#[cfg(test)]
pub mod testutil;
