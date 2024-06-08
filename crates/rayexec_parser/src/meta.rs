use std::fmt::Debug;

use crate::ast::{CommonTableExpr, DataType, FunctionArg, Ident, ObjectReference};

/// Metadata associated with sql statements.
///
/// During parsing, a 'raw' implementation of this will be used which represents
/// the input provided by the user with minimal processing.
///
/// During binding, the 'raw' statement will be pushed through a binder,
/// resulting in a 'bound' statement with an implementation of `AstMeta` that
/// provides representations than can be used during planning, including
/// resolved tables and types.
pub trait AstMeta: Clone {
    /// Name of a data source for ATTACH.
    type DataSourceName: Debug + Clone + PartialEq;

    type ItemReference: Debug + Clone + PartialEq;

    type TableReference: Debug + Clone + PartialEq;

    /// Reference to a table function.
    type TableFunctionReference: Debug + Clone + PartialEq;

    /// Arguments to a table function.
    type TableFunctionArguments: Debug + Clone + PartialEq;

    type CteReference: Debug + Clone + PartialEq;

    /// Reference to a scalar or aggregate function.
    type FunctionReference: Debug + Clone + PartialEq;

    /// Reference to a column.
    type ColumnReference: Debug + Clone + PartialEq;

    /// A data type.
    type DataType: Debug + Clone + PartialEq;
}

/// The raw representation of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Raw;

impl AstMeta for Raw {
    type DataSourceName = Ident;
    type ItemReference = ObjectReference;
    type TableReference = ObjectReference;
    type TableFunctionReference = ObjectReference;
    type TableFunctionArguments = Vec<FunctionArg<Raw>>;
    type CteReference = CommonTableExpr<Raw>;
    type FunctionReference = ObjectReference;
    type ColumnReference = Ident;
    type DataType = DataType;
}
