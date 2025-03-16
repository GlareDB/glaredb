use std::sync::Arc;

use rayexec_parser::ast;

use crate::catalog::database::AttachInfo;
use crate::catalog::entry::CatalogEntry;
use crate::functions::table::PlannedTableFunction;

/// Table or CTE found in the FROM clause.
#[derive(Debug, Clone)]
pub enum ResolvedTableOrCteReference {
    /// Resolved table.
    Table(ResolvedTableReference),
    /// Resolved view.
    View(ResolvedViewReference),
    /// Resolved CTE.
    ///
    /// Stores the normalized name of the CTE so that it can be looked up during
    /// binding.
    Cte(String),
}

#[derive(Debug, Clone)]
pub struct ResolvedTableReference {
    pub catalog: String,
    pub schema: String,
    pub entry: Arc<CatalogEntry>,
    pub scan_function: PlannedTableFunction,
}

#[derive(Debug, Clone)]
pub struct ResolvedViewReference {
    pub catalog: String,
    pub schema: String,
    pub entry: Arc<CatalogEntry>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedTableReference {
    /// The raw ast reference.
    pub reference: ast::ObjectReference,
    /// Name of the catalog this table is in.
    pub catalog: String,
    /// How we attach the catalog.
    ///
    /// Currently it's expected that this is always Some (we get attach info
    /// from the client), but there's a path where this can be None, and attach
    /// info gets injected on the server-side. Right now, the server will error
    /// if this is None.
    pub attach_info: Option<AttachInfo>,
}
