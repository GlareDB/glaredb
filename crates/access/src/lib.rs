//! Provides data access.
//!
//! Codename cachemoney.
//!
//! This crate interacts heavily with the `persistence` crate.
//!
//! # Vocabulary
//!
//! - **Record Batch**: A group of records within the same schema.
//! - **Partition**: Multiple record batches making up a part of table.
//! - **Table**: Multiple partitions that once combined, make up an entire "user
//!   table".
//! - **Delta**: A change to some partition.
#![allow(dead_code)]
pub mod compact;
pub mod deltacache;
pub mod errors;
pub mod exec;
pub mod partition;
pub mod runtime;
pub mod strategy;
pub mod table;

mod parquet;
