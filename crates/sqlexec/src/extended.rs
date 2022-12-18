#![allow(dead_code)]
use crate::errors::internal;
use crate::{errors::Result, logical_plan::LogicalPlan};
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

// A prepared statement.
// This is contains the SQL statements that will later be turned into a
// portal when a Bind message is received.
#[derive(Debug)]
pub struct PreparedStatement {
    pub sql: String,
    pub param_types: Vec<i32>,
    pub(crate) statement: ast::Statement,
}

impl PreparedStatement {
    pub fn new(sql: String, param_types: Vec<i32>) -> Result<Self> {
        // TODO: parse the SQL for placeholders
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, &sql)?
            .into_iter()
            .collect::<Vec<ast::Statement>>();

        // Ensure that the SQL is a single statement
        if statements.len() != 1 {
            return Err(internal!(
                "Only single SQL statements are supported, got {}",
                statements.len()
            ));
        }

        // TODO: validate/infer the param_types
        //
        Ok(Self {
            sql,
            param_types,
            statement: statements[0].clone(),
        })
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
