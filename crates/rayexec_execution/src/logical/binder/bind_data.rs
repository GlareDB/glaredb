use rayexec_error::{not_implemented, OptionExt, RayexecError, Result};
use rayexec_parser::ast::{self};
use rayexec_proto::ProtoConv;
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::{
    database::DatabaseContext, logical::operator::LocationRequirement, proto::DatabaseProtoConv,
};

use super::{
    bound_cte::BoundCte,
    bound_function::BoundFunction,
    bound_table::{BoundTableOrCteReference, CteIndex},
    bound_table_function::{BoundTableFunctionReference, UnboundTableFunctionReference},
};

/// Data that's collected during binding, including resolved tables, functions,
/// and other database objects.
///
/// Planning will reference these items directly instead of having to resolve
/// them.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BindData {
    /// A bound table may reference either an actual table, or a CTE. An unbound
    /// reference may only reference a table.
    pub tables: BindList<BoundTableOrCteReference, ast::ObjectReference>,

    /// Bound scalar or aggregate functions.
    pub functions: BindList<BoundFunction, ast::ObjectReference>,

    /// Bound (and planned) table functions. Unbound table functions include the
    /// table function arguments to allow for quick planning on the remote side.
    pub table_functions: BindList<BoundTableFunctionReference, UnboundTableFunctionReference>,

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
    pub ctes: Vec<BoundCte>,
}

impl BindData {
    /// Checks if there's any unbound references in this query's bind data.
    pub fn any_unbound(&self) -> bool {
        self.tables.any_unbound()
            || self.functions.any_unbound()
            || self.table_functions.any_unbound()
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
    pub fn find_cte(&self, name: &str) -> Option<CteIndex> {
        let mut search_depth = self.current_depth;

        for (idx, cte) in self.ctes.iter().rev().enumerate() {
            if cte.depth > search_depth {
                // We're looking another subquery's CTEs.
                return None;
            }

            if cte.name == name {
                // We found a good reference.
                return Some(CteIndex(self.ctes.len() - 1 - idx)); // Since we're iterating backwards.
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
    pub fn push_cte(&mut self, cte: BoundCte) -> CteIndex {
        let idx = self.ctes.len();
        self.ctes.push(cte);
        CteIndex(idx)
    }
}

impl DatabaseProtoConv for BindData {
    type ProtoType = rayexec_proto::generated::binder::BindData;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        if !self.ctes.is_empty() {
            // More ast work needed.
            not_implemented!("encode ctes in bind data")
        }

        Ok(Self::ProtoType {
            tables: Some(self.tables.to_proto_ctx(context)?),
            functions: Some(self.functions.to_proto_ctx(context)?),
            table_functions: Some(self.table_functions.to_proto_ctx(context)?),
            current_depth: self.current_depth as u32,
        })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        Ok(Self {
            tables: BindList::from_proto_ctx(proto.tables.required("tables")?, context)?,
            functions: BindList::from_proto_ctx(proto.functions.required("functions")?, context)?,
            table_functions: BindList::from_proto_ctx(
                proto.table_functions.required("table_functions")?,
                context,
            )?,
            current_depth: proto.current_depth as usize,
            ctes: Vec::new(),
        })
    }
}

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

/// List for holding bound and unbound variants for a single logical concept
/// (table, function, etc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindList<B, U> {
    pub inner: Vec<MaybeBound<B, U>>,
}

/// Index into the bind list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BindListIdx(pub usize);

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

impl DatabaseProtoConv for BindList<BoundTableOrCteReference, ast::ObjectReference> {
    type ProtoType = rayexec_proto::generated::binder::TablesBindList;

    fn to_proto_ctx(&self, _context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::binder::{
            maybe_bound_table::Value, BoundTableOrCteReferenceWithLocation, MaybeBoundTable,
        };

        let mut tables = Vec::new();
        for table in &self.inner {
            let table = match table {
                MaybeBound::Bound(bound, loc) => MaybeBoundTable {
                    value: Some(Value::Bound(BoundTableOrCteReferenceWithLocation {
                        bound: Some(bound.to_proto()?),
                        location: loc.to_proto()? as i32,
                    })),
                },
                MaybeBound::Unbound(unbound) => MaybeBoundTable {
                    value: Some(Value::Unbound(unbound.to_proto()?)),
                },
            };
            tables.push(table);
        }

        Ok(Self::ProtoType { tables })
    }

    fn from_proto_ctx(proto: Self::ProtoType, _context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::binder::maybe_bound_table::Value;

        let tables = proto
            .tables
            .into_iter()
            .map(|t| match t.value.required("value")? {
                Value::Bound(bound) => {
                    let location = LocationRequirement::from_proto(bound.location())?;
                    let bound =
                        BoundTableOrCteReference::from_proto(bound.bound.required("bound")?)?;
                    Ok(MaybeBound::Bound(bound, location))
                }
                Value::Unbound(unbound) => Ok(MaybeBound::Unbound(
                    ast::ObjectReference::from_proto(unbound)?,
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { inner: tables })
    }
}

impl DatabaseProtoConv for BindList<BoundFunction, ast::ObjectReference> {
    type ProtoType = rayexec_proto::generated::binder::FunctionsBindList;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::binder::{
            maybe_bound_function::Value, BoundFunctionReferenceWithLocation, MaybeBoundFunction,
        };

        let mut funcs = Vec::new();
        for func in &self.inner {
            let func = match func {
                MaybeBound::Bound(bound, loc) => MaybeBoundFunction {
                    value: Some(Value::Bound(BoundFunctionReferenceWithLocation {
                        bound: Some(bound.to_proto_ctx(context)?),
                        location: loc.to_proto()? as i32,
                    })),
                },
                MaybeBound::Unbound(unbound) => MaybeBoundFunction {
                    value: Some(Value::Unbound(unbound.to_proto()?)),
                },
            };
            funcs.push(func);
        }

        Ok(Self::ProtoType { functions: funcs })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::binder::maybe_bound_function::Value;

        let funcs = proto
            .functions
            .into_iter()
            .map(|f| match f.value.required("value")? {
                Value::Bound(bound) => {
                    let location = LocationRequirement::from_proto(bound.location())?;
                    let bound =
                        BoundFunction::from_proto_ctx(bound.bound.required("bound")?, context)?;
                    Ok(MaybeBound::Bound(bound, location))
                }
                Value::Unbound(unbound) => Ok(MaybeBound::Unbound(
                    ast::ObjectReference::from_proto(unbound)?,
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { inner: funcs })
    }
}

impl DatabaseProtoConv for BindList<BoundTableFunctionReference, UnboundTableFunctionReference> {
    type ProtoType = rayexec_proto::generated::binder::TableFunctionsBindList;

    fn to_proto_ctx(&self, context: &DatabaseContext) -> Result<Self::ProtoType> {
        use rayexec_proto::generated::binder::{
            maybe_bound_table_function::Value, BoundTableFunctionReferenceWithLocation,
            MaybeBoundTableFunction,
        };

        let mut funcs = Vec::new();
        for func in &self.inner {
            let func = match func {
                MaybeBound::Bound(bound, loc) => MaybeBoundTableFunction {
                    value: Some(Value::Bound(BoundTableFunctionReferenceWithLocation {
                        bound: Some(bound.to_proto_ctx(context)?),
                        location: loc.to_proto()? as i32,
                    })),
                },
                MaybeBound::Unbound(unbound) => MaybeBoundTableFunction {
                    value: Some(Value::Unbound(unbound.to_proto()?)),
                },
            };
            funcs.push(func);
        }

        Ok(Self::ProtoType { functions: funcs })
    }

    fn from_proto_ctx(proto: Self::ProtoType, context: &DatabaseContext) -> Result<Self> {
        use rayexec_proto::generated::binder::maybe_bound_table_function::Value;

        let funcs = proto
            .functions
            .into_iter()
            .map(|f| match f.value.required("value")? {
                Value::Bound(bound) => {
                    let location = LocationRequirement::from_proto(bound.location())?;
                    let bound = BoundTableFunctionReference::from_proto_ctx(
                        bound.bound.required("bound")?,
                        context,
                    )?;
                    Ok(MaybeBound::Bound(bound, location))
                }
                Value::Unbound(unbound) => Ok(MaybeBound::Unbound(
                    UnboundTableFunctionReference::from_proto(unbound)?,
                )),
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { inner: funcs })
    }
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

impl ProtoConv for ItemReference {
    type ProtoType = rayexec_proto::generated::binder::ItemReference;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            idents: self.0.clone(),
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self(proto.idents))
    }
}
