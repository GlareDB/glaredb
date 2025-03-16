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

//! Parquet schema definitions and methods to print and parse schema.
//!
//! * [`SchemaDescriptor`] describes the data types of the columns stored in a file
//! * [`ColumnDescriptor`]: Describes the schema of a single (leaf) column.
//! * [`ColumnPath`]: Represents the location of a column in the schema (e.g. a nested field)
//!
//! Parquet distinguishes
//! between "logical" and "physical" data types.
//! For instance, strings (logical type) are stored as byte arrays (physical type).
//! Likewise, temporal types like dates, times, timestamps, etc. (logical type)
//! are stored as integers (physical type).
//!
//! [`SchemaDescriptor`]: types::SchemaDescriptor
//! [`ColumnDescriptor`]: types::ColumnDescriptor
//! [`ColumnPath`]: types::ColumnPath

pub mod parser;
pub mod printer;
pub mod types;
pub mod visitor;
