use crate::{
    errors::Result,
    logical_plan::LogicalPlan,
};

// A prepared statement.
// This is contains the SQL statements that will later be turned into a
// portal when a Bind message is received.
#[derive(Debug)]
pub struct PreparedStatement {
    pub sql: String,
    pub param_types: Vec<i32>,
}

impl PreparedStatement {
    pub fn new(sql: String, param_types: Vec<i32>) -> Self {
        // TODO: parse the SQL for placeholders
        Self { sql, param_types }
    }

    /// The Describe message statement variant returns a ParameterDescription message describing
    /// the parameters needed by the statement, followed by a RowDescription message describing the
    /// rows that will be returned when the statement is eventually executed.
    /// If the statement will not return rows, then a NoData message is returned.
    pub fn describe(&self) {
        // since bind has not been issued, the formats to be used for returned columns are not yet
        // known. In this case, the backend will assume the default format (text) for all columns.
        todo!("describe statement")
    }
}

/// A Portal is the result of a prepared statement that has been bound with the Bind message.
/// The portal is a readied execution plan that can be executed using an Execute message.
#[derive(Debug)]
pub struct Portal {
    pub plan: LogicalPlan,
    pub param_formats: Vec<i16>,
    pub param_values: Vec<Option<Vec<u8>>>,
    pub result_formats: Vec<i16>,
}

impl Portal {
    pub fn new(
        plan: LogicalPlan,
        param_formats: Vec<i16>,
        param_values: Vec<Option<Vec<u8>>>,
        result_formats: Vec<i16>,
    ) -> Result<Self> {
        Ok(Self {
            plan,
            param_formats,
            param_values,
            result_formats,
        })
    }
}
