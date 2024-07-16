use std::fmt::Debug;

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::ast::{CommonTableExpr, CopyToTarget, DataType, FunctionArg, Ident, ObjectReference};

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
    type DataSourceName: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    type ItemReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    type TableReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference to a table function.
    type TableFunctionReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Arguments to a table function.
    type TableFunctionArguments: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    type CteReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference to a scalar or aggregate function.
    type FunctionReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference to a column.
    type ColumnReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// A data type.
    type DataType: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    type CopyToDestination: Debug + Clone + PartialEq + Serialize + DeserializeOwned;
}

/// The raw representation of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    type CopyToDestination = CopyToTarget;
}
