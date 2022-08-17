//! High-level SQL query planning and execution.
//!
//! # Life of a query
//!
//! The first stop for a query entering the system is parsing. Parsing is done
//! via the sqlparser-rs library using the Postgres dialect option.
//!
//! If the query is transaction related ("commit", "begin", "rollback"), the
//! session for that connection executes the appropriate functions for creating,
//! committing, or rolling back the transaction. The result of which is then
//! returned to the client, and the query is finished.
//!
//! DML queries ("select", "insert", etc) and DDL queries ("create table", etc)
//! go through a planning phase which attempts to reconcile table names and other
//! identifiers with physical items in the database as reported by the catalog.
//! If the catalog errors for whatever reason (missing table, etc), the query
//! exits. During planning, the query is transformed to an equivalent
//! intermediate representation with appropriately bound references to tables and
//! whatnot. This intermediate representation is very close to lemur's
//! `RelationExpr`.
//!
//! Once the query is planned and we have our intermediate representation, we
//! perform some rule based rewrites for a more optimal query. Additional
//! optimization will be added in the future.
//!
//! After optimization, the intermediate representation is lowered in to a lemur
//! `RelationExpr`. It is this final expression that gets executed. The result of
//! execution is returned to the client.
//!
//! # Transactions
//!
//! Every query must execute within the context of a transaction. If the user
//! hasn't started a transaction, a non-interactive transaction will be created
//! and used. If the user has started a transaction, that transaction will be
//! used until the user commits or rolls back the transaction.
#![allow(dead_code)]
pub mod catalog;
pub mod engine;
pub mod plan;
pub mod server;
pub mod system;
