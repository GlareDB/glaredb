//! Server runtime parameters.
//!
//! See <https://www.postgresql.org/docs/current/config-setting.html> for
//! documentation on Postgres parameters. GlareDB will support a small fraction
//! of Postgres parameters along with GlareDB-only parameters.
use crate::errors::{internal, Result};
use datafusion::sql::sqlparser::ast;
use std::collections::HashMap;

// TODO: Some of this should be moved to a 'catalog' crate (along with other
// catalog related functionality).

pub const SEARCH_PATH_PARAM: &str = "search_path";
pub const DUMMY_PARAM: &str = "dummy";

/// Session local parameters.
///
/// NOTE: This does not track transaction local variables.
#[derive(Debug, Default)]
pub struct SessionParameters {
    parameters: HashMap<String, ParameterValue>,
}

impl SessionParameters {
    /// Set a session local parameter.
    pub fn set_parameter(
        &mut self,
        variable: ast::ObjectName,
        values: Vec<ast::Expr>,
    ) -> Result<()> {
        let name = variable.to_string();
        let value = ParameterValue::try_new(&name, values)?;
        self.parameters.insert(name, value);
        Ok(())
    }

    pub fn get_parameter(&self, name: &str) -> Option<&ParameterValue> {
        self.parameters.get(name)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum ParameterValue {
    Strings(Vec<String>),
    String(String),
}

impl ParameterValue {
    /// Try to create a parameter setting from the given variable name and
    /// values.
    ///
    /// Errors if trying to set an unsupported variable or if the values are not
    /// what the variable expects.
    fn try_new(variable: &str, values: Vec<ast::Expr>) -> Result<ParameterValue> {
        Ok(match variable {
            SEARCH_PATH_PARAM => ParameterValue::Strings(parse_strings(values)?),
            DUMMY_PARAM => ParameterValue::String(get_single_string(values)?),
            other => return Err(internal!("unexpected parameter name: {}", other)),
        })
    }
}

/// Parse string expressions into a vec
fn parse_strings(values: Vec<ast::Expr>) -> Result<Vec<String>> {
    let mut strings = Vec::with_capacity(values.len());
    for value in values {
        let ident = match value {
            ast::Expr::Identifier(ident) => ident.to_string(),
            other => return Err(internal!("unexpected expression: {:?}", other)),
        };
        // TODO: This is not handling single quoted strings right now.
        // E.g. 'public,"testing"' should be parsed as vec!["public", "testing"]
        strings.push(ident);
    }

    Ok(strings)
}

fn get_single_string(mut values: Vec<ast::Expr>) -> Result<String> {
    if values.len() != 1 {
        return Err(internal!("invalid number of values: {:?}", values));
    }
    Ok(match values.pop().unwrap() {
        ast::Expr::Identifier(ident) => ident.to_string(),
        other => return Err(internal!("unexpected expression: {:?}", other)),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_parameter() {
        struct Test {
            variable: &'static str,
            values: Vec<ast::Expr>,
            output: Option<ParameterValue>,
        }

        let tests = vec![
            Test {
                variable: "dummy",
                values: vec![ast::Expr::Identifier("hello".into())],
                output: Some(ParameterValue::String("hello".to_string())),
            },
            Test {
                variable: "search_path",
                values: vec![
                    ast::Expr::Identifier("public".into()),
                    ast::Expr::Identifier("testing".into()),
                ],
                output: Some(ParameterValue::Strings(vec![
                    "public".to_string(),
                    "testing".to_string(),
                ])),
            },
            Test {
                variable: "missing",
                values: vec![ast::Expr::Identifier("public,testing".into())],
                output: None,
            },
        ];

        for test in tests {
            let result = ParameterValue::try_new(test.variable, test.values);
            match (result, test.output) {
                (Ok(got), Some(expected)) => assert_eq!(got, expected),
                (Err(_), None) => (),
                (got, expected) => panic!("got: {:?}, expected: {:?}", got, expected),
            }
        }
    }
}
