use std::fmt;

use glaredb_error::{DbError, Result};
use glaredb_parser::ast::{self};
use glaredb_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use super::resolved_cte::ResolvedCte;
use super::resolved_function::ResolvedFunction;
use super::resolved_table::{ResolvedTableOrCteReference, UnresolvedTableReference};
use super::resolved_table_function::{
    ResolvedTableFunctionReference,
    UnresolvedTableFunctionReference,
};
use crate::logical::operator::LocationRequirement;

/// Context containing resolved database objects.
#[derive(Debug, Clone, Default)]
pub struct ResolveContext {
    /// A resolved table may reference either an actual table, or a CTE. An
    /// unresolved reference may only reference a table.
    pub tables: ResolveList<ResolvedTableOrCteReference, UnresolvedTableReference>,

    /// Resolved scalar or aggregate functions.
    pub functions: ResolveList<ResolvedFunction, ast::ObjectReference>,

    /// Resolved (and planned) table functions. Unbound table functions include
    /// the table function arguments to allow for quick planning on the remote
    /// side.
    pub table_functions:
        ResolveList<ResolvedTableFunctionReference, UnresolvedTableFunctionReference>,

    // /// An optional COPY TO for the query.
    // ///
    // /// Currently this only supports a local COPY TO (the result needs to be
    // /// local, the inner query can be local or remote). Extending this to
    // /// support remote COPY TO should be straightforward, we just have to figure
    // /// out what the "unbound" variant should be since it's not directly
    // /// referenced by the user (maybe file format?).
    // pub copy_to: Option<ResolvedCopyTo>,
    /// How "deep" in the plan are we.
    ///
    /// Incremented everytime we dive into a subquery.
    ///
    /// This provides a primitive form of scoping for CTE resolution.
    pub current_depth: usize,

    /// CTEs are appended to the vec as they're encountered.
    ///
    /// When search for a CTE, the vec should be iterated from right to left to
    /// try to get the "closest" CTE to the reference.
    pub ctes: Vec<ResolvedCte>,
}

impl ResolveContext {
    pub const fn empty() -> Self {
        ResolveContext {
            tables: ResolveList::empty(),
            functions: ResolveList::empty(),
            table_functions: ResolveList::empty(),
            current_depth: 0,
            ctes: Vec::new(),
        }
    }

    /// Checks if there's any unresolved references in this query.
    pub fn any_unresolved(&self) -> bool {
        self.tables.any_unresolved()
            || self.functions.any_unresolved()
            || self.table_functions.any_unresolved()
    }

    /// Try to find a CTE by its normalized name.
    ///
    /// This will iterate the cte vec right to left to find best cte that
    /// matches this name.
    ///
    /// The current depth will be used to determine if a CTE is valid to
    /// reference or not. What this means is as we iterate, we can go "up" in
    /// depth, but never back down, as going back down would mean we're
    /// attempting to resolve a cte from a "sibling" subquery.
    // TODO: This doesn't account for CTEs defined in sibling subqueries yet
    // that happen to have the same name and depths _and_ there's no CTEs in the
    // parent.
    pub fn find_cte(&self, name: &str) -> Option<&ResolvedCte> {
        let mut search_depth = self.current_depth;

        for cte in self.ctes.iter().rev() {
            if cte.depth > search_depth {
                // We're looking another subquery's CTEs.
                return None;
            }

            if cte.name == name {
                // We found a good reference.
                return Some(cte);
            }

            // Otherwise keep searching, even if the cte is up a level.
            search_depth = cte.depth;
        }

        // No CTE found.
        None
    }

    pub fn inc_depth(&mut self) {
        self.current_depth += 1
    }

    pub fn dec_depth(&mut self) {
        self.current_depth -= 1;
    }

    /// Push a CTE into bind data, returning a CTE reference.
    pub fn push_cte(&mut self, cte: ResolvedCte) {
        self.ctes.push(cte);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaybeResolved<B, U> {
    /// The object has been resolved, and has a given location requirement.
    Resolved(B, LocationRequirement),
    /// Object is unresolved.
    Unresolved(U),
}

impl<B, U> MaybeResolved<B, U> {
    pub const fn is_resolved(&self) -> bool {
        matches!(self, MaybeResolved::Resolved(_, _))
    }

    pub fn try_unwrap_resolved(self) -> Result<(B, LocationRequirement)> {
        match self {
            Self::Resolved(b, loc) => Ok((b, loc)),
            Self::Unresolved(_) => Err(DbError::new("Reference not resolved")),
        }
    }
}

/// List for holding resolved and unresolved variants for a single database
/// object type (table, function, etc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveList<B, U> {
    pub inner: Vec<MaybeResolved<B, U>>,
}

/// Index into the resolve list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveListIdx(pub usize);

impl<B, U> ResolveList<B, U> {
    pub const fn empty() -> Self {
        ResolveList { inner: Vec::new() }
    }

    pub fn any_unresolved(&self) -> bool {
        self.inner
            .iter()
            .any(|v| matches!(v, MaybeResolved::Unresolved(_)))
    }

    pub fn try_get_bound(
        &self,
        ResolveListIdx(idx): ResolveListIdx,
    ) -> Result<(&B, LocationRequirement)> {
        match self.inner.get(idx) {
            Some(MaybeResolved::Resolved(b, loc)) => Ok((b, *loc)),
            Some(MaybeResolved::Unresolved(_)) => Err(DbError::new("Item not resolved")),
            None => Err(DbError::new("Missing reference")),
        }
    }

    pub fn push_maybe_resolved(&mut self, maybe: MaybeResolved<B, U>) -> ResolveListIdx {
        let idx = self.inner.len();
        self.inner.push(maybe);
        ResolveListIdx(idx)
    }

    pub fn push_resolved(&mut self, bound: B, loc: LocationRequirement) -> ResolveListIdx {
        self.push_maybe_resolved(MaybeResolved::Resolved(bound, loc))
    }

    pub fn push_unresolved(&mut self, unbound: U) -> ResolveListIdx {
        self.push_maybe_resolved(MaybeResolved::Unresolved(unbound))
    }
}

impl<B, U> Default for ResolveList<B, U> {
    fn default() -> Self {
        Self { inner: Vec::new() }
    }
}

// TODO: Figure out how we want to represent things like tables in a CREATE
// TABLE. We don't want to resolve, so a vec of strings works for now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemReference(pub Vec<String>);

impl ItemReference {
    pub fn pop(&mut self) -> Result<String> {
        // TODO: Could be more informative with this error.
        self.0.pop().ok_or_else(|| DbError::new("End of reference"))
    }

    pub fn pop_2(&mut self) -> Result<[String; 2]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| DbError::new("Expected 2 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| DbError::new("Expected 2 identifiers, got 1"))?;
        Ok([b, a])
    }

    pub fn pop_3(&mut self) -> Result<[String; 3]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| DbError::new("Expected 3 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| DbError::new("Expected 3 identifiers, got 1"))?;
        let c = self
            .0
            .pop()
            .ok_or_else(|| DbError::new("Expected 3 identifiers, got 2"))?;
        Ok([c, b, a])
    }
}

impl From<Vec<String>> for ItemReference {
    fn from(value: Vec<String>) -> Self {
        ItemReference(value)
    }
}

impl fmt::Display for ItemReference {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join(","))
    }
}

impl ProtoConv for ItemReference {
    type ProtoType = glaredb_proto::generated::resolver::ItemReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            idents: self.0.clone(),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self(proto.idents))
    }
}
