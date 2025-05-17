use std::collections::{HashMap, HashSet};
use std::fmt;

use glaredb_error::{DbError, Result};

use super::bind_query::BoundQuery;
use super::ident::BinderIdent;
use super::table_list::{Table, TableAlias, TableList, TableRef};
use crate::arrays::datatype::DataType;
use crate::expr::Expression;
use crate::expr::column_expr::ColumnReference;
use crate::logical::operator::{LogicalNode, LogicalOperator};

/// Reference to a child bind scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindScopeRef {
    pub context_idx: usize,
}

/// Reference to a materialization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MaterializationRef {
    pub materialization_idx: usize,
}

impl fmt::Display for MaterializationRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MAT_{}", self.materialization_idx)
    }
}

/// Reference to a CTE.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CteRef {
    pub cte_idx: usize,
}

impl fmt::Display for CteRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CTE_{}", self.cte_idx)
    }
}

/// Bind context hold information about "table-producing" operators during
/// planning.
///
/// When we go through initial logical planning, the bind context will have
/// tables added to it, and is referenced when determining what's in scope.
///
/// The bind context will also be provided to the optimizer and any changes to
/// the plans (e.g. columns removed from table scans) will be updated in the
/// context.
///
/// Physical planning will then use the bind context for determining physical
/// column ordering.
// TODO: Move more of the table/table list handling into TableList
#[derive(Debug)]
pub struct BindContext {
    /// All child scopes used for binding.
    ///
    /// Initialized with a single scope (root).
    scopes: Vec<BindScope>,
    /// Table list for the query.
    tables: TableList,
    /// All CTEs in the query.
    ///
    /// Referenced via `CteRef`.
    ctes: Vec<BoundCte>,
    /// All plans that will be materialized.
    ///
    /// Referenced via `MaterializationRef`.
    materializations: Vec<PlanMaterialization>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CorrelatedColumn {
    /// Reference to an outer context the column is referencing.
    pub outer: BindScopeRef,
    pub table: TableRef,
    /// Index of the column in the table.
    pub col_idx: usize,
}

#[derive(Debug, Clone)]
pub struct BoundCte {
    /// Scope used for binding the CTE.
    pub bind_scope: BindScopeRef,
    /// If this CTE should be materialized.
    pub materialized: bool,
    /// Normalized name for the CTE.
    pub name: BinderIdent,
    /// Column names, possibly aliased.
    pub column_names: Vec<BinderIdent>,
    /// Column types.
    pub column_types: Vec<DataType>,
    /// The bound plan representing the CTE.
    pub bound: Box<BoundQuery>,
    /// Materialization reference for the CTE.
    ///
    /// If `materialized` is false and this is None, we need to plan the bound
    /// query first.
    pub mat_ref: Option<MaterializationRef>,
}

#[derive(Debug, Clone)]
pub struct UsingColumn {
    /// Normalized column name.
    pub column: BinderIdent,
    /// A reference to one of the tables used in the USING condition.
    pub table_ref: TableRef,
    /// Column index inside the table.
    pub col_idx: usize,
}

#[derive(Debug, Clone, Default)]
struct BindScope {
    /// Index to the parent bind context.
    ///
    /// Will be None if this is the root context.
    parent: Option<BindScopeRef>,
    /// Correlated columns in the query at this depth.
    correlated_columns: Vec<CorrelatedColumn>,
    /// Columns that are used in a USING join condition.
    using_columns: Vec<UsingColumn>,
    /// Tables currently in scope.
    tables: Vec<TableRef>,
    /// CTEs in scope. Keyed by normalized CTE name.
    ctes: HashMap<BinderIdent, CteRef>,
}

/// A node in the logical plan that will be materialized to allow for multiple
/// scans.
#[derive(Debug)]
pub struct PlanMaterialization {
    pub mat_ref: MaterializationRef,
    /// Plan we'll be materializing.
    // TODO: This should be an Option so we can take it once during physical
    // planning instead of needing to clone it.
    pub plan: LogicalOperator,
    /// Number of scans against this plan.
    pub scan_count: usize,
    /// Table references for the output of this plan.
    // TODO: Why do we need this?
    pub table_refs: Vec<TableRef>,
}

impl BindContext {
    /// Creates a new empty bind context with a single root-level scope.
    pub fn new_for_root() -> Self {
        BindContext {
            scopes: vec![BindScope {
                parent: None,
                tables: Vec::new(),
                correlated_columns: Vec::new(),
                using_columns: Vec::new(),
                ctes: HashMap::new(),
            }],
            tables: TableList::empty(),
            ctes: Vec::new(),
            materializations: Vec::new(),
        }
    }

    pub fn root_scope_ref(&self) -> BindScopeRef {
        BindScopeRef { context_idx: 0 }
    }

    pub fn get_table_list(&self) -> &TableList {
        &self.tables
    }

    /// Creates a new bind scope, with current being the parent scope.
    ///
    /// The resulting scope should have visibility into parent scopes (for
    /// binding correlated columns).
    pub fn new_child_scope(&mut self, current: BindScopeRef) -> BindScopeRef {
        let idx = self.scopes.len();
        self.scopes.push(BindScope {
            parent: Some(current),
            tables: Vec::new(),
            correlated_columns: Vec::new(),
            using_columns: Vec::new(),
            ctes: HashMap::new(),
        });

        BindScopeRef { context_idx: idx }
    }

    /// Creates a new scope that has no parents, and thus no visibility into any
    /// other scope.
    pub fn new_orphan_scope(&mut self) -> BindScopeRef {
        let idx = self.scopes.len();
        self.scopes.push(BindScope {
            parent: None,
            tables: Vec::new(),
            correlated_columns: Vec::new(),
            using_columns: Vec::new(),
            ctes: HashMap::new(),
        });

        BindScopeRef { context_idx: idx }
    }

    /// Adds a CTE to the current scope.
    ///
    /// Errors on duplicate CTE name.
    pub fn add_cte(&mut self, current: BindScopeRef, cte: BoundCte) -> Result<CteRef> {
        let idx = self.ctes.len();

        let scope = self.get_scope_mut(current)?;
        if scope.ctes.contains_key(&cte.name) {
            return Err(DbError::new(format!("Duplicate CTE name '{}'", cte.name)));
        }

        let cte_ref = CteRef { cte_idx: idx };
        scope.ctes.insert(cte.name.clone(), cte_ref);

        self.ctes.push(cte);

        Ok(cte_ref)
    }

    /// Try to find CTE by name.
    ///
    /// If CTE is not found in the current scope, the parent scope will be
    /// search (all the way up to the root of the query).
    pub fn find_cte(&self, current: BindScopeRef, name: &str) -> Result<CteRef> {
        let scope = self.get_scope(current)?;

        match scope.ctes.get(name) {
            Some(cte) => Ok(*cte),
            None => {
                let parent = match self.get_parent_ref(current)? {
                    Some(parent) => parent,
                    None => return Err(DbError::new(format!("Missing CTE '{name}'"))),
                };

                self.find_cte(parent, name)
            }
        }
    }

    pub fn get_cte(&self, cte_ref: CteRef) -> Result<&BoundCte> {
        self.ctes
            .get(cte_ref.cte_idx)
            .ok_or_else(|| DbError::new(format!("Missing CTE for ref: {cte_ref}")))
    }

    pub fn get_cte_mut(&mut self, cte_ref: CteRef) -> Result<&mut BoundCte> {
        self.ctes
            .get_mut(cte_ref.cte_idx)
            .ok_or_else(|| DbError::new(format!("Missing CTE for ref: {cte_ref}")))
    }

    /// Adds a plan for materialization to the bind context.
    ///
    /// Scan count for the materialization is initially set to 0.
    pub fn new_materialization(&mut self, plan: LogicalOperator) -> Result<MaterializationRef> {
        // TODO: Dedup with subquery decorrelation.
        let plan_tables = plan.get_output_table_refs(self);
        let idx = self.materializations.len();
        let mat_ref = MaterializationRef {
            materialization_idx: idx,
        };

        self.materializations.push(PlanMaterialization {
            mat_ref,
            plan,
            scan_count: 0,
            table_refs: plan_tables,
        });

        Ok(mat_ref)
    }

    pub fn inc_materialization_scan_count(
        &mut self,
        mat_ref: MaterializationRef,
        by: usize,
    ) -> Result<()> {
        let mat = self.get_materialization_mut(mat_ref)?;
        mat.scan_count += by;
        Ok(())
    }

    pub fn get_materialization_mut(
        &mut self,
        mat_ref: MaterializationRef,
    ) -> Result<&mut PlanMaterialization> {
        self.materializations
            .get_mut(mat_ref.materialization_idx)
            .ok_or_else(|| {
                DbError::new(format!(
                    "Missing materialization for idx {}",
                    mat_ref.materialization_idx
                ))
            })
    }

    pub fn get_materialization(&self, mat_ref: MaterializationRef) -> Result<&PlanMaterialization> {
        self.materializations
            .get(mat_ref.materialization_idx)
            .ok_or_else(|| {
                DbError::new(format!(
                    "Missing materialization for idx {}",
                    mat_ref.materialization_idx
                ))
            })
    }

    /// Iterates plan materializations in the order they were planned, returning
    /// mut references.
    pub fn iter_materializations_mut(
        &mut self,
    ) -> impl Iterator<Item = &mut PlanMaterialization> + '_ {
        self.materializations.iter_mut()
    }

    /// Iterates plan materializations in the order they were planned.
    pub fn iter_materializations(&self) -> impl Iterator<Item = &PlanMaterialization> + '_ {
        self.materializations.iter()
    }

    pub fn get_parent_ref(&self, bind_ref: BindScopeRef) -> Result<Option<BindScopeRef>> {
        let child = self.get_scope(bind_ref)?;
        Ok(child.parent)
    }

    pub fn table_is_in_scope(&self, current: BindScopeRef, table_ref: TableRef) -> Result<bool> {
        let current = self.get_scope(current)?;
        Ok(current.tables.contains(&table_ref))
    }

    pub fn correlated_columns(&self, bind_ref: BindScopeRef) -> Result<&Vec<CorrelatedColumn>> {
        let child = self.get_scope(bind_ref)?;
        Ok(&child.correlated_columns)
    }

    /// Appends correlated column from some other scope to current scope.
    pub fn append_correlated_columns(
        &mut self,
        current: BindScopeRef,
        from: BindScopeRef,
    ) -> Result<()> {
        let mut other_correlated = self.get_scope(from)?.correlated_columns.clone();
        let current = self.get_scope_mut(current)?;
        current.correlated_columns.append(&mut other_correlated);
        Ok(())
    }

    /// Appends `other` context to `current`.
    ///
    /// Errors on duplicate table aliases.
    pub fn append_context(&mut self, current: BindScopeRef, other: BindScopeRef) -> Result<()> {
        let left_aliases: HashSet<_> = self
            .iter_tables_in_scope(current)?
            .filter_map(|t| t.alias.as_ref())
            .collect();

        for right_alias in self
            .iter_tables_in_scope(other)?
            .filter_map(|t| t.alias.as_ref())
        {
            if left_aliases.contains(right_alias) {
                return Err(DbError::new(format!(
                    "Duplicate table name: {}",
                    right_alias
                )));
            }
        }

        let (mut other_tables, mut other_using, mut other_correlations) = {
            let other = self.get_scope(other)?;
            (
                other.tables.clone(),
                other.using_columns.clone(),
                other.correlated_columns.clone(),
            )
        };

        let current = self.get_scope_mut(current)?;

        current.tables.append(&mut other_tables);
        current.using_columns.append(&mut other_using);
        current.correlated_columns.append(&mut other_correlations);

        Ok(())
    }

    /// Removes the given table from a context, taking those tables out of
    /// scope.
    pub fn remove_tables(&mut self, current: BindScopeRef, tables: &[TableRef]) -> Result<()> {
        let current = self.get_scope_mut(current)?;
        current.tables.retain_mut(|v| !tables.contains(v));

        Ok(())
    }

    /// Computes distance from child to parent, erroring if there's no
    /// connection between the refs.
    ///
    /// Counts "edges" between contexts, so the immediate parent of a child
    /// context will have a distance of 1.
    pub fn distance_child_to_parent(
        &self,
        child: BindScopeRef,
        parent: BindScopeRef,
    ) -> Result<usize> {
        let mut current = self.get_scope(child)?;
        let mut distance = 0;

        loop {
            distance += 1;
            let current_parent = match current.parent {
                Some(current_parent) => {
                    if parent == current_parent {
                        return Ok(distance);
                    }
                    current_parent
                }
                None => {
                    return Err(DbError::new(
                        "No connection between child and parent context",
                    ));
                }
            };

            current = self.get_scope(current_parent)?;
        }
    }

    /// Create a table that belong to no scope.
    pub fn new_ephemeral_table(&mut self) -> Result<TableRef> {
        self.new_ephemeral_table_with_columns::<String>([], [])
    }

    pub fn new_ephemeral_table_with_columns<S>(
        &mut self,
        column_types: impl IntoIterator<Item = DataType>,
        column_names: impl IntoIterator<Item = S>,
    ) -> Result<TableRef>
    where
        S: Into<BinderIdent>,
    {
        self.tables.push_table(None, column_types, column_names)
    }

    /// Creates a new table with generated columns from an iterator of expression.
    pub fn new_ephemeral_table_from_expressions<'a>(
        &mut self,
        generated_prefix: &str,
        exprs_iter: impl Iterator<Item = &'a Expression>,
    ) -> Result<TableRef> {
        let column_types = exprs_iter
            .map(|expr| expr.datatype())
            .collect::<Result<Vec<_>>>()?;

        self.new_ephemeral_table_from_types(generated_prefix, column_types)
    }

    /// Creates a new table with generated column from a list of datatypes.
    pub fn new_ephemeral_table_from_types(
        &mut self,
        generated_prefix: &str,
        types: Vec<DataType>,
    ) -> Result<TableRef> {
        let names = (0..types.len()).map(|idx| format!("{generated_prefix}_{idx}"));
        self.new_ephemeral_table_with_columns(types, names)
    }

    pub fn push_column_for_table(
        &mut self,
        table: TableRef,
        name: impl Into<String>,
        datatype: DataType,
    ) -> Result<usize> {
        let table = self.get_table_mut(table)?;
        let idx = table.column_types.len();
        table
            .column_names
            .push(BinderIdent::new(name.into(), false));
        table.column_types.push(datatype);
        Ok(idx)
    }

    pub fn get_column(&self, reference: impl Into<ColumnReference>) -> Result<(&str, &DataType)> {
        self.tables.get_column(reference)
    }

    pub fn get_column_type(&self, reference: impl Into<ColumnReference>) -> Result<DataType> {
        self.tables.get_column_type(reference)
    }

    pub fn get_table(&self, table_ref: TableRef) -> Result<&Table> {
        self.tables.get(table_ref)
    }

    pub fn get_table_mut(&mut self, table_ref: TableRef) -> Result<&mut Table> {
        self.tables.get_mut(table_ref)
    }

    pub fn push_table<S>(
        &mut self,
        bind_ref: BindScopeRef,
        alias: Option<TableAlias>,
        column_types: impl IntoIterator<Item = DataType>,
        column_names: impl IntoIterator<Item = S>,
    ) -> Result<TableRef>
    where
        S: Into<BinderIdent>,
    {
        if let Some(alias) = &alias {
            // If we have multiple tables in scope, they need to have unique
            // alias (e.g. by ensure one is more qualified than the other)
            for have_alias in self
                .iter_tables_in_scope(bind_ref)?
                .filter_map(|t| t.alias.as_ref())
            {
                if have_alias == alias {
                    return Err(DbError::new(format!("Duplicate table name: {alias}")));
                }
            }
        }

        let table_ref = self.tables.push_table(alias, column_types, column_names)?;
        let scope = self.get_scope_mut(bind_ref)?;
        scope.tables.push(table_ref);

        Ok(table_ref)
    }

    pub fn append_table_to_scope(&mut self, scope: BindScopeRef, table: TableRef) -> Result<()> {
        // TODO: Probably check columns for duplicates.
        let scope = self.get_scope_mut(scope)?;
        scope.tables.push(table);
        Ok(())
    }

    pub fn push_correlation(
        &mut self,
        idx: BindScopeRef,
        correlation: CorrelatedColumn,
    ) -> Result<()> {
        let child = self.get_scope_mut(idx)?;
        child.correlated_columns.push(correlation);
        Ok(())
    }

    pub fn push_correlations(
        &mut self,
        idx: BindScopeRef,
        correlations: impl IntoIterator<Item = CorrelatedColumn>,
    ) -> Result<()> {
        let scope = self.get_scope_mut(idx)?;
        for corr in correlations {
            scope.correlated_columns.push(corr);
        }
        Ok(())
    }

    /// Tries to find the the scope that has a matching column name.
    ///
    /// This first searches any USING columns if `alias` is None, then proceeds
    /// to search all tables in this scope. Outer scopes are not searched.
    ///
    /// Returns the table reference containing the column, and the relative
    /// index of the column within that table.
    ///
    /// `cmp` determines if column matching is case sensitive or not.
    pub fn find_table_for_column(
        &self,
        current: BindScopeRef,
        alias: Option<&TableAlias>,
        lookup: &BinderIdent,
    ) -> Result<Option<(TableRef, usize)>> {
        if alias.is_none() {
            let using = self
                .get_using_columns(current)?
                .iter()
                .find(|&using| using.column.strict_eq(lookup));
            if let Some(using) = using {
                return Ok(Some((using.table_ref, using.col_idx)));
            }
        }

        let mut found = None;

        for table in self.iter_tables_in_scope(current)? {
            match (&table.alias, &alias) {
                (Some(a1), Some(a2)) => {
                    if !a1.matches(a2) {
                        continue;
                    }
                }
                (None, Some(_)) => continue,
                _ => (),
            }

            for (col_idx, col_name) in table.column_names.iter().enumerate() {
                if col_name.strict_eq(lookup) {
                    if found.is_some() {
                        return Err(DbError::new(format!("Ambiguous column name '{lookup}'")));
                    }
                    found = Some((table.reference, col_idx));
                }
            }
        }

        Ok(found)
    }

    /// Iterate tables in the given bind scope.
    pub fn iter_tables_in_scope(
        &self,
        current: BindScopeRef,
    ) -> Result<impl Iterator<Item = &Table>> {
        let context = self.get_scope(current)?;
        Ok(context
            .tables
            .iter()
            .map(|table| &self.tables.tables[table.table_idx]))
    }

    /// Appends a USING column to the current scope.
    pub fn append_using_column(&mut self, current: BindScopeRef, col: UsingColumn) -> Result<()> {
        let scope = self.get_scope_mut(current)?;
        scope.using_columns.push(col);
        Ok(())
    }

    pub fn get_using_columns(&self, current: BindScopeRef) -> Result<&[UsingColumn]> {
        let scope = self.get_scope(current)?;
        Ok(&scope.using_columns)
    }

    fn get_scope(&self, bind_ref: BindScopeRef) -> Result<&BindScope> {
        self.scopes
            .get(bind_ref.context_idx)
            .ok_or_else(|| DbError::new("Missing child bind context"))
    }

    fn get_scope_mut(&mut self, bind_ref: BindScopeRef) -> Result<&mut BindScope> {
        self.scopes
            .get_mut(bind_ref.context_idx)
            .ok_or_else(|| DbError::new("Missing child bind context"))
    }
}

#[cfg(test)]
pub(crate) mod testutil {
    //! Test utilities for the bind context.

    use super::*;

    /// Collect all (name, type) pairs for columns in the current scope.
    pub fn columns_in_scope(
        bind_context: &BindContext,
        scope: BindScopeRef,
    ) -> Vec<(String, DataType)> {
        bind_context
            .iter_tables_in_scope(scope)
            .unwrap()
            .flat_map(|t| {
                t.column_names
                    .iter()
                    .map(|s| s.as_raw_str().to_string())
                    .zip(t.column_types.iter().cloned())
            })
            .collect()
    }
}
