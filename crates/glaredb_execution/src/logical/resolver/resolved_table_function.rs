use std::collections::HashMap;

use rayexec_parser::ast;

use crate::arrays::scalar::ScalarValue;
use crate::functions::function_set::TableFunctionSet;
use crate::functions::table::{PlannedTableFunction, TableFunctionInput};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstantFunctionArgs {
    pub positional: Vec<ScalarValue>,
    pub named: HashMap<String, ScalarValue>,
}

#[derive(Debug, Clone)]
pub enum ResolvedTableFunctionReference {
    /// Table function contains constant arguments so we were able to plan it
    /// during the resolve step.
    ///
    /// It's currently requires that scan functions can be planned during
    /// resolve, as scan functions have async bind functions.
    Planned(PlannedTableFunction),
    /// Table function contains non-constant arguments, so planning happens
    /// after resolving.
    Delayed(TableFunctionSet),
}

impl ResolvedTableFunctionReference {
    pub fn base_table_alias(&self) -> String {
        match self {
            ResolvedTableFunctionReference::Planned(func) => func.name.to_string(),
            ResolvedTableFunctionReference::Delayed(func) => func.name.to_string(),
        }
    }
}

/// An unresolved reference to a table function.
#[derive(Debug, Clone, PartialEq)]
pub struct UnresolvedTableFunctionReference {
    /// Original reference in the ast.
    pub reference: ast::ObjectReference,
    /// Constant arguments to the function.
    ///
    /// This currently assumes that any unresolved table function is for trying
    /// to do a scan on a data source that's not registered (e.g.
    /// `read_postgres` from wasm).
    // TODO: Optionally set this? There's a possibility that the remote side has
    // an in/out function that we don't know about, and so these args aren't
    // actually needed. Not urgent to figure out right now.
    pub args: TableFunctionInput,
}
