use rayexec_error::{RayexecError, Result};
use std::fmt;
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ident<'a> {
    pub value: &'a str,
}

impl<'a> fmt::Display for Ident<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectReference<'a>(pub Vec<Ident<'a>>);

impl<'a> fmt::Display for ObjectReference<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let strings: Vec<_> = self.0.iter().map(|ident| ident.value.to_string()).collect();
        write!(f, "{}", strings.join("."))
    }
}

#[derive(Debug, Clone)]
pub enum Statement<'a> {
    /// CREATE SCHEMA ...
    CreateSchema {
        reference: ObjectReference<'a>,
        if_not_exists: bool,
    },
}
