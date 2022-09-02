//! Lemur provides data representations for the system and execution operations.
//!
//! The choice of "lemur" comes from the homemade acronym LMIR, which itself is
//! a combination of LIR (low-level intermediate representation) and MIR
//! (mid-level intermediate representation).
//!
//! # Data representation
//!
//! A partial goal of lemur is to provide and MIR and LIR for the system. The
//! core type that much of the system will be concerned with is the `DataFrame`.
//! `DataFrame`s are made up of an ordered list of value vectors, with each
//! value vector being a homogeneous list of values of the same type. During
//! expression evaluation, expressions will be evaluated against entire
//! dataframes.
//!
//! # Expressions
//!
//! Lemur provides two categories of expressions, scalar expressions, and
//! relational expressions. A scalar expression is an expression that logically
//! acts on an individual values for each row. For example, adding two columns
//! together would be a scalar expression. On the other hand, relational
//! expressions are expressions that logically act on entire relations at a
//! time. This includes things like joins and filtering.
//!
//! During query planning, the goal of the sqlengine is to transform a SQL ast
//! into a single relation expression. It is this expression that then gets
//! executed.
//!
//! # Execution
//!
//! Once we have a relational expression, we can then start execution. Each node
//! of the expression tree operates in a pull-based manner, with each node
//! producing a stream of dataframes to operate on.
//!
//! Execution is completed when the top-level dataframe stream completes.
#![allow(dead_code)]
pub mod arrow;
pub mod errors;
pub mod execute;
pub mod repr;
