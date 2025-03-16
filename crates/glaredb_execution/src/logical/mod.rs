//! Module for planning SQL statements.
//!
//! Planning phases:
//!
//! 1. Resolve
//! 2. Bind
//! 3. Plan
//!
//! # Resolving
//!
//! First step in SQL planning. Walks the AST to resolve all tables and functions in
//! the query. This retains the structure of the AST and annotates tables and
//! functions with their catalog entries.
//!
//! Resolving is async to allow for hitting remote resources (e.g. determining the
//! schema of file, or contacting an external database).
//!
//! This step also determines if we should be flipped to hybrid execution. When
//! walking the AST, unknown references will be tracked. At the the end of
//! resolving, we check for any unknown references, and if we have any, we send the
//! statement a remote server. The remote side should complete resolving, and
//! continue on with the remaining plan steps.
//!
//! # Binding
//!
//! Walk the AST and transform the query into something more amenable to planning.
//!
//! During this phase, we build up a bind context which tracks "table-producing"
//! steps within the query. This context is the basis for determining scoping within
//! the query.
//!
//! The bound statement produces is a halfway point between the AST and logical
//! plan.
//!
//! # Planning
//!
//! Walks the bound statement and produces a logical plan.

pub mod operator;
pub mod scan_filter;
pub mod statistics;

pub mod binder;
pub mod planner;
pub mod resolver;

pub mod logical_aggregate;
pub mod logical_attach;
pub mod logical_copy;
pub mod logical_create;
pub mod logical_describe;
pub mod logical_distinct;
pub mod logical_drop;
pub mod logical_empty;
pub mod logical_explain;
pub mod logical_expression_list;
pub mod logical_filter;
pub mod logical_inout;
pub mod logical_insert;
pub mod logical_join;
pub mod logical_limit;
pub mod logical_materialization;
pub mod logical_order;
pub mod logical_project;
pub mod logical_scan;
pub mod logical_set;
pub mod logical_setop;
pub mod logical_unnest;
pub mod logical_window;
