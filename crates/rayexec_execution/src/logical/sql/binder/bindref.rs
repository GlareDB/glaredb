use rayexec_error::{RayexecError, Result};
use rayexec_parser::{ast, meta::Raw};
use serde::{
    de::DeserializeSeed, ser::SerializeTupleVariant, Deserialize, Deserializer, Serialize,
    Serializer,
};
use std::{fmt, marker::PhantomData};

use crate::{
    database::{entry::TableEntry, DatabaseContext},
    functions::{
        aggregate::AggregateFunction,
        scalar::ScalarFunction,
        table::{PlannedTableFunction, TableFunctionArgs},
    },
    logical::operator::LocationRequirement,
};

/// Index into the bind list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindListIdx(pub usize);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MaybeBound<B, U> {
    /// The object has been bound, and has a given location requirement.
    Bound(B, LocationRequirement),
    /// Object is unbound.
    Unbound(U),
}

impl<B, U> MaybeBound<B, U> {
    pub const fn is_bound(&self) -> bool {
        matches!(self, MaybeBound::Bound(_, _))
    }

    pub fn try_unwrap_bound(self) -> Result<(B, LocationRequirement)> {
        match self {
            Self::Bound(b, loc) => Ok((b, loc)),
            Self::Unbound(_) => Err(RayexecError::new("Bind reference is not bound")),
        }
    }
}

impl<B: Serialize, U: Serialize> Serialize for MaybeBound<B, U> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Bound(bound, loc) => {
                let mut s = serializer.serialize_tuple_variant("MaybeBound", 0, "Bound", 2)?;
                s.serialize_field(bound)?;
                s.serialize_field(loc)?;
                s.end()
            }
            Self::Unbound(unbound) => {
                let mut s = serializer.serialize_tuple_variant("MaybeBound", 1, "Unbound", 1)?;
                s.serialize_field(unbound)?;
                s.end()
            }
        }
    }
}

#[derive(Debug)]
pub struct MaybeBoundDeserializer<'a, B, U> {
    pub context: &'a DatabaseContext,
    pub _bound: PhantomData<B>,
    pub _unbound: PhantomData<U>,
}

impl<'de, 'a> DeserializeSeed<'de>
    for MaybeBoundDeserializer<'a, TableOrCteReference, ast::ObjectReference>
{
    type Value = MaybeBound<TableOrCteReference, ast::ObjectReference>;

    fn deserialize<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        unimplemented!()
    }
}

/// List for holding bound and unbound variants for a single logical concept
/// (table, function, etc).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BindList<B, U> {
    pub inner: Vec<MaybeBound<B, U>>,
}

/// Bind list for table like objects (tables or CTEs).
pub type TableBindList = BindList<TableOrCteReference, ast::ObjectReference>;

/// Bind list for functions (scalar or aggs).
pub type FunctionBindList = BindList<FunctionReference, ast::ObjectReference>;

/// Bind list for table functions.
pub type TableFunctionBindList = BindList<TableFunctionReference, ast::FromTableFunction<Raw>>;

impl<B, U> BindList<B, U> {
    pub fn any_unbound(&self) -> bool {
        self.inner
            .iter()
            .any(|v| matches!(v, MaybeBound::Unbound(_)))
    }

    pub fn try_get_bound(&self, idx: BindListIdx) -> Result<(&B, LocationRequirement)> {
        match self.inner.get(idx.0) {
            Some(MaybeBound::Bound(b, loc)) => Ok((b, *loc)),
            Some(MaybeBound::Unbound(_)) => Err(RayexecError::new("Item not bound")),
            None => Err(RayexecError::new("Missing bind item")),
        }
    }

    pub fn push_maybe_bound(&mut self, maybe: MaybeBound<B, U>) -> BindListIdx {
        let idx = self.inner.len();
        self.inner.push(maybe);
        BindListIdx(idx)
    }

    pub fn push_bound(&mut self, bound: B, loc: LocationRequirement) -> BindListIdx {
        self.push_maybe_bound(MaybeBound::Bound(bound, loc))
    }

    pub fn push_unbound(&mut self, unbound: U) -> BindListIdx {
        self.push_maybe_bound(MaybeBound::Unbound(unbound))
    }
}

impl<B, U> Default for BindList<B, U> {
    fn default() -> Self {
        Self { inner: Vec::new() }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum FunctionReference {
    Scalar(Box<dyn ScalarFunction>),
    Aggregate(Box<dyn AggregateFunction>),
}

impl FunctionReference {
    /// Get the name of the function.
    ///
    /// Used when generating column names if user doesn't provide an alias.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Scalar(f) => f.name(),
            Self::Aggregate(f) => f.name(),
        }
    }
}

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableOrCteReference {
    /// Resolved table.
    Table {
        catalog: String,
        schema: String,
        entry: TableEntry,
    },
    /// Resolved CTE.
    Cte(CteReference),
}

/// References a CTE that can be found in `BindData`.
///
/// Note that this doesn't hold the CTE itself since it may be referenced more
/// than once in a query.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CteReference {
    /// Index into the CTE map.
    pub idx: usize,
}

/// Table function found in the FROM clause.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TableFunctionReference {
    /// Name of the original function.
    ///
    /// This is used to allow the user to reference the output of the function
    /// if not provided an alias.
    pub name: String,

    /// The initialized table function.
    pub func: Box<dyn PlannedTableFunction>,

    /// Arguments to the function.
    pub args: TableFunctionArgs,
}

// TODO: Figure out how we want to represent things like tables in a CREATE
// TABLE. We don't want to resolve, so a vec of strings works for now.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemReference(pub Vec<String>);

impl ItemReference {
    pub fn pop(&mut self) -> Result<String> {
        // TODO: Could be more informative with this error.
        self.0
            .pop()
            .ok_or_else(|| RayexecError::new("End of reference"))
    }

    pub fn pop_2(&mut self) -> Result<[String; 2]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 2 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 2 identifiers, got 1"))?;
        Ok([b, a])
    }

    pub fn pop_3(&mut self) -> Result<[String; 3]> {
        let a = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 0"))?;
        let b = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 1"))?;
        let c = self
            .0
            .pop()
            .ok_or_else(|| RayexecError::new("Expected 3 identifiers, got 2"))?;
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
