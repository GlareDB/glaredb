//! Contains logic for converting raw SQL statement into a logical plan.
//!
//! Planning happens in two stages:
//!
//! 1. Statement resolve
//! 2. Logical planning
//!
//! During 'statement resolve', the AST is walked, and database objects are
//! resolved from the database context. Since resolving potentially external
//! database objects is async, the entire binding phase is async. Statement
//! resolving does not attempt to verify that the user's query is correct. Its
//! primary job is to make sure the query is annotated with everything it needs
//! such that the planner does not need to reference the catalog.
//!
//! 'Logical planning' takes a bound statement, and produces a logical plan from
//! that statement. It does not require access to any external resources, as
//! everything's that's needed is already annotated on the query. It's at this
//! step when query correctness and scoping is checked.
//!
//! Future:
//!
//! - Inferrence step between binding and planning. If a catalog says that it
//!   would prefer than we try to infer the schema of a table from the query
//!   context, we should be able to do that (for many queries, there might be
//!   edge cases where doing that is intractable).
//! - Allow partial binding. If a query attempts to reference something that
//!   this instance/session is not aware of, it should continue to bind as much
//!   as it can. Once the statement is returned, we can send it off to an
//!   instance (the cloud) this is able to complete the binding process. This is
//!   for hybrid exec.
pub mod scope;

pub mod plan_from;
pub mod plan_statement;

mod plan_copy;
mod plan_create_table;
mod plan_explain;
mod plan_insert;
mod plan_query;
mod plan_select;
mod plan_setop;
mod plan_subquery;
mod plan_unnest;
