use crate::errors::{ExecError, Result};
use jsoncat::catalog::Catalog;
use jsoncat::constants::DEFAULT_SCHEMA;
use jsoncat::transaction::Context;
use lazy_static::lazy_static;
use regex::Regex;

/// Regex for matching strings delineated by commas. Will match full quoted
/// strings as well.
///
/// Taken from <https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes>
const SPLIT_ON_UNQUOTED_COMMAS: &str = r#""[^"]*"|[^,]+"#;

#[derive(Debug)]
pub struct SearchPath {
    schemas: Vec<String>,
}

impl SearchPath {
    pub fn new() -> SearchPath {
        SearchPath {
            schemas: vec![DEFAULT_SCHEMA.to_string()],
        }
    }

    /// Try to set the search path, ensuring that each schema exists for this
    /// context.
    ///
    /// No modifications are made on error.
    pub fn try_set<C: Context>(&mut self, ctx: &C, catalog: &Catalog, text: &str) -> Result<()> {
        let schemas = split_search_paths(text);
        for schema in &schemas {
            if !catalog.schema_exists(ctx, schema) {
                return Err(ExecError::InvalidSearchPath {
                    input: text.to_string(),
                    schema: schema.to_string(),
                });
            }
        }

        self.schemas = schemas;
        Ok(())
    }

    /// Iterate over the schema names in the search path.
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.schemas.iter().map(|s| s.as_str())
    }
}

fn split_search_paths(text: &str) -> Vec<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new(SPLIT_ON_UNQUOTED_COMMAS).unwrap();
    }

    RE.find_iter(text)
        .into_iter()
        .map(|m| m.as_str().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_paths() {
        struct Test {
            input: &'static str,
            expected: Vec<String>,
        }

        let tests = vec![
            Test {
                input: "",
                expected: Vec::new(),
            },
            Test {
                input: "my_schema",
                expected: vec!["my_schema".to_string()],
            },
            Test {
                input: "a,b,c",
                expected: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            Test {
                input: "a,\"b,c\"",
                expected: vec!["a".to_string(), "\"b,c\"".to_string()],
            },
        ];

        for test in tests {
            let out = split_search_paths(test.input);
            assert_eq!(test.expected, out);
        }
    }
}
