use std::fmt::Debug;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::ast::{CopyOption, CopyToTarget, DataType, ObjectReference, ShowReference};

/// Metadata associated with sql statements.
///
/// During parsing, a 'raw' implementation of this will be used which represents
/// the input provided by the user with minimal processing.
///
/// During binding, the 'raw' statement will be pushed through a resolver,
/// resulting in a 'resolved' statement with an implementation of `AstMeta` that
/// provides representations than can be used during planning, including
/// resolved tables and types.
pub trait AstMeta: Clone {
    /// Reference to item that might not have any associated context with it.
    type ItemReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference to a table.
    type TableReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference to a table function.
    type TableFunctionReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference to a scalar or aggregate function.
    type FunctionReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Extra options for subqueries.
    ///
    /// This is used to provide extra scoping for when we resolve views. Views
    /// get converted to subqueries during the resolve step (and so we don't
    /// need to special case if a view happens to be hybrid query).
    ///
    /// A normal subquery is able to reference columns outside of it, but a view
    /// cannot. This type provides a way to indicate that.
    type SubqueryOptions: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// A data type.
    type DataType: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Destination for a COPY TO statement.
    type CopyToDestination: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Options provided in a COPY TO statement.
    type CopyToOptions: Debug + Clone + PartialEq + Serialize + DeserializeOwned;

    /// Reference for SHOW commands.
    type ShowReference: Debug + Clone + PartialEq + Serialize + DeserializeOwned;
}

/// The raw representation of a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Raw;

impl AstMeta for Raw {
    type ItemReference = ObjectReference;
    type TableReference = ObjectReference;
    type TableFunctionReference = ObjectReference;
    type FunctionReference = ObjectReference;
    type SubqueryOptions = ();
    type DataType = DataType;
    type CopyToDestination = CopyToTarget;
    type CopyToOptions = Vec<CopyOption<Raw>>;
    type ShowReference = ShowReference;
}
