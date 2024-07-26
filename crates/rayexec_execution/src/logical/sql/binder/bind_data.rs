use rayexec_error::{RayexecError, Result};
use rayexec_parser::ast::{self, QueryNode};
use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    ser::{SerializeMap, SerializeSeq},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::fmt;

use crate::{
    database::{entry::TableEntry, DatabaseContext},
    functions::{
        aggregate::AggregateFunction,
        scalar::ScalarFunction,
        table::{PlannedTableFunction, TableFunctionArgs},
    },
    logical::operator::LocationRequirement,
    serde::{
        AggregateFunctionDeserializer, PlannedTableFunctionDeserializer, ScalarFunctionDeserializer,
    },
};

use super::Bound;

/// Data that's collected during binding, including resolved tables, functions,
/// and other database objects.
///
/// Planning will reference these items directly instead of having to resolve
/// them.
///
/// Note that there's a bit of indirection between "bound" objects and actual
/// references to the objects. This mostly exists to make implementing statefule
/// (de)serialization easier since the things that require state are in the
/// `ObjectList`s at the top level. If we didn't have that indirection, manually
/// implementing (de)serialization for all the types that would need it would
/// make this file double in size. I (Sean) am not opposed to that if we find
/// that gets in the way, but I didn't want to spend time on that right now
/// especially since this stuff is still evolving.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BindData {
    /// Scalar functions referenced from bound function references.
    pub scalar_function_objects: ObjectList<Box<dyn ScalarFunction>>,

    /// Aggregate functions referenced from bound function references.
    pub aggregate_function_objects: ObjectList<Box<dyn AggregateFunction>>,

    // TODO: This may change to just have `dyn TableFunction` references, and
    // then have a separate step after binding that initializes all table
    // functions.
    pub table_function_objects: ObjectList<Box<dyn PlannedTableFunction>>,

    /// A bound table may reference either an actual table, or a CTE. An unbound
    /// reference may only reference a table.
    pub tables: BindList<BoundTableOrCteReference, ast::ObjectReference>,

    /// Bound scalar or aggregate functions.
    pub functions: BindList<BoundFunctionReference, ast::ObjectReference>,

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
    pub fn find_cte(&self, name: &str) -> Option<CteReference> {
        let mut search_depth = self.current_depth;

        for (idx, cte) in self.ctes.iter().rev().enumerate() {
            if cte.depth > search_depth {
                // We're looking another subquery's CTEs.
                return None;
            }

            if cte.name == name {
                // We found a good reference.
                return Some(CteReference {
                    idx: (self.ctes.len() - 1) - idx, // Since we're iterating backwards.
                });
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
    pub fn push_cte(&mut self, cte: BoundCte) -> CteReference {
        let idx = self.ctes.len();
        self.ctes.push(cte);
        CteReference { idx }
    }
}

impl Serialize for BindData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("scalar_function_objects", &self.scalar_function_objects.0)?;
        map.serialize_entry(
            "aggregate_function_objects",
            &self.aggregate_function_objects.0,
        )?;
        map.serialize_entry("table_function_objects", &self.table_function_objects.0)?;
        map.serialize_entry("tables", &self.tables)?;
        map.serialize_entry("functions", &self.functions)?;
        map.serialize_entry("table_functions", &self.table_functions)?;
        map.serialize_entry("current_depth", &self.current_depth)?;
        map.serialize_entry("ctes", &self.ctes)?;
        map.end()
    }
}

pub struct BindDataVisitor<'a> {
    pub context: &'a DatabaseContext,
}

impl<'de, 'a> Visitor<'de> for BindDataVisitor<'a> {
    type Value = BindData;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bind data")
    }

    fn visit_map<V>(self, mut map: V) -> Result<BindData, V::Error>
    where
        V: MapAccess<'de>,
    {
        let mut scalar_function_objects = None;
        let mut aggregate_function_objects = None;
        let mut table_function_objects = None;
        let mut tables = None;
        let mut functions = None;
        let mut table_functions = None;
        let mut current_depth = None;
        let mut ctes = None;

        while let Some(key) = map.next_key()? {
            match key {
                "scalar_function_objects" => {
                    scalar_function_objects = Some(map.next_value_seed(ObjectListVisitor {
                        visitor: ScalarFunctionDeserializer {
                            context: self.context,
                        },
                    })?)
                }
                "aggregate_function_objects" => {
                    aggregate_function_objects = Some(map.next_value_seed(ObjectListVisitor {
                        visitor: AggregateFunctionDeserializer {
                            context: self.context,
                        },
                    })?)
                }
                "table_function_objects" => {
                    table_function_objects = Some(map.next_value_seed(ObjectListVisitor {
                        visitor: PlannedTableFunctionDeserializer {
                            context: self.context,
                        },
                    })?)
                }
                "tables" => {
                    tables = Some(map.next_value()?);
                }
                "functions" => {
                    functions = Some(map.next_value()?);
                }
                "table_functions" => {
                    table_functions = Some(map.next_value()?);
                }
                "current_depth" => {
                    current_depth = Some(map.next_value()?);
                }
                "ctes" => {
                    ctes = Some(map.next_value()?);
                }
                _ => {
                    let _ = map.next_value::<de::IgnoredAny>()?;
                }
            }
        }

        let scalar_function_objects = scalar_function_objects
            .ok_or_else(|| de::Error::missing_field("scalar_function_objects"))?;
        let aggregate_function_objects = aggregate_function_objects
            .ok_or_else(|| de::Error::missing_field("aggregate_function_objects"))?;
        let table_function_objects = table_function_objects
            .ok_or_else(|| de::Error::missing_field("table_function_objects"))?;
        let tables = tables.ok_or_else(|| de::Error::missing_field("tables"))?;
        let functions = functions.ok_or_else(|| de::Error::missing_field("functions"))?;
        let table_functions =
            table_functions.ok_or_else(|| de::Error::missing_field("table_functions"))?;
        let current_depth =
            current_depth.ok_or_else(|| de::Error::missing_field("current_depth"))?;
        let ctes = ctes.ok_or_else(|| de::Error::missing_field("ctes"))?;

        Ok(BindData {
            scalar_function_objects,
            aggregate_function_objects,
            table_function_objects,
            tables,
            functions,
            table_functions,
            current_depth,
            ctes,
        })
    }
}

impl<'de, 'a> DeserializeSeed<'de> for BindDataVisitor<'a> {
    type Value = BindData;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectIdx(pub usize);

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectList<T>(Vec<T>);

impl<T> ObjectList<T> {
    pub fn push(&mut self, val: T) -> ObjectIdx {
        let idx = self.0.len();
        self.0.push(val);
        ObjectIdx(idx)
    }

    pub fn get(&self, idx: ObjectIdx) -> Result<&T> {
        self.0
            .get(idx.0)
            .ok_or_else(|| RayexecError::new("Missing object"))
    }
}

impl<T> Default for ObjectList<T> {
    fn default() -> Self {
        ObjectList(Vec::new())
    }
}

impl<T: Serialize> Serialize for ObjectList<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for v in &self.0 {
            seq.serialize_element(v)?;
        }
        seq.end()
    }
}

struct ObjectListVisitor<I> {
    visitor: I,
}

impl<'de, I, T> Visitor<'de> for ObjectListVisitor<I>
where
    I: DeserializeSeed<'de, Value = T> + Copy,
{
    type Value = ObjectList<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a list of values")
    }

    fn visit_seq<V>(self, mut seq: V) -> Result<ObjectList<T>, V::Error>
    where
        V: SeqAccess<'de>,
    {
        let mut inner = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(value) = seq.next_element_seed(self.visitor)? {
            inner.push(value);
        }

        Ok(ObjectList(inner))
    }
}

impl<'de, I, T> DeserializeSeed<'de> for ObjectListVisitor<I>
where
    I: DeserializeSeed<'de, Value = T> + Copy,
{
    type Value = ObjectList<T>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

/// A bound aggregate or scalar function.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundFunctionReference {
    /// Index into the scalar objects list.
    Scalar(ObjectIdx),
    /// Index into the aggregate objects list.
    Aggregate(ObjectIdx),
}

/// A bound table function reference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundTableFunctionReference {
    /// Name of the original function.
    ///
    /// This is used to allow the user to reference the output of the function
    /// if not provided an alias.
    pub name: String,
    /// Index into the table function objects list.
    pub idx: ObjectIdx,
    // TODO: Maybe keep args here?
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UnboundTableFunctionReference {
    /// Original reference in the ast.
    pub reference: ast::ObjectReference,
    /// Arguments to the function.
    ///
    /// Note that these are required to be constant and so we don't need to
    /// delay binding.
    pub args: TableFunctionArgs,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BoundTableOrCteReference {
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

// TODO: This might need some scoping information.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoundCte {
    /// Normalized name for the CTE.
    pub name: String,
    /// Depth this CTE was found at.
    pub depth: usize,
    /// Column aliases taken directly from the ast.
    pub column_aliases: Option<Vec<ast::Ident>>,
    /// The bound query node.
    pub body: QueryNode<Bound>,
    /// If this CTE should be materialized.
    pub materialized: bool,
}
