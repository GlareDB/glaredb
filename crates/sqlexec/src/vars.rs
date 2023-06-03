//! Server and session variables.
use crate::errors::{ExecError, Result};
use datafusion::arrow::array::StringArray;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use metastorebuiltin::DEFAULT_SCHEMA;
use once_cell::sync::Lazy;
use regex::Regex;
use std::borrow::{Borrow, ToOwned};
use std::sync::Arc;

// TODO: Decide proper postgres version to spoof/support
const SERVER_VERSION: ServerVar<str> = ServerVar {
    name: "server_version",
    value: "15.1",
};

const APPLICATION_NAME: ServerVar<str> = ServerVar {
    name: "application_name",
    value: "",
};

const CLIENT_ENCODING: ServerVar<str> = ServerVar {
    name: "client_encoding",
    value: "UTF8",
};

const EXTRA_FLOAT_DIGITS: ServerVar<i32> = ServerVar {
    name: "extra_float_digits",
    value: &1,
};

const STATEMENT_TIMEOUT: ServerVar<i32> = ServerVar {
    name: "statement_timeout",
    value: &0,
};

const TIMEZONE: ServerVar<str> = ServerVar {
    name: "TimeZone",
    value: "UTC",
};

const DATESTYLE: ServerVar<str> = ServerVar {
    name: "DateStyle",
    value: "ISO",
};

const TRANSACTION_ISOLATION: ServerVar<str> = ServerVar {
    name: "transaction_isolation",
    value: "read uncommitted",
};

static DEFAULT_SEARCH_PATH: Lazy<[String; 1]> = Lazy::new(|| [DEFAULT_SCHEMA.to_owned()]);
static SEARCH_PATH: Lazy<ServerVar<[String]>> = Lazy::new(|| ServerVar {
    name: "search_path",
    value: &*DEFAULT_SEARCH_PATH,
});

// GlareDB specific.
static GLAREDB_VERSION_OWNED: Lazy<String> =
    Lazy::new(|| format!("v{}", env!("CARGO_PKG_VERSION")));
static GLAREDB_VERSION: Lazy<ServerVar<str>> = Lazy::new(|| ServerVar {
    name: "glaredb_version",
    value: &GLAREDB_VERSION_OWNED,
});

// GlareDB specific.
const ENABLE_DEBUG_DATASOURCES: ServerVar<bool> = ServerVar {
    name: "enable_debug_datasources",
    value: &false,
};

// GlareDB specific.
const FORCE_CATALOG_REFRESH: ServerVar<bool> = ServerVar {
    name: "force_catalog_refresh",
    value: &false,
};

/// Variables for a session.
///
/// Variables that can be changed are of the `SessionVar` type, and default to
/// the system default if left unset. Variables that cannot be changed are of
/// the `SystemVar` type.
pub struct SessionVars {
    pub server_version: ServerVar<str>,
    pub application_name: SessionVar<str>,
    pub client_encoding: SessionVar<str>,
    pub extra_floating_digits: SessionVar<i32>,
    pub statement_timeout: SessionVar<i32>,
    pub timezone: SessionVar<str>,
    pub datestyle: SessionVar<str>,
    pub transaction_isolation: ServerVar<str>,
    pub search_path: SessionVar<[String]>,
    pub enable_debug_datasources: SessionVar<bool>,
    pub force_catalog_refresh: SessionVar<bool>,
    pub glaredb_version: &'static ServerVar<str>,
}

impl SessionVars {
    /// Return an iterator to the variables that should be sent to the client on
    /// session start.
    pub fn startup_vars_iter(&self) -> impl Iterator<Item = &dyn AnyVar> {
        let vars: [&dyn AnyVar; 3] = [
            &self.server_version,
            &self.application_name,
            &self.client_encoding,
        ];
        vars.into_iter()
    }

    /// Get a value for a variable.
    pub fn get(&self, name: &str) -> Result<&dyn AnyVar> {
        if name.eq_ignore_ascii_case(SERVER_VERSION.name) {
            Ok(&self.server_version)
        } else if name.eq_ignore_ascii_case(APPLICATION_NAME.name) {
            Ok(&self.application_name)
        } else if name.eq_ignore_ascii_case(CLIENT_ENCODING.name) {
            Ok(&self.client_encoding)
        } else if name.eq_ignore_ascii_case(EXTRA_FLOAT_DIGITS.name) {
            Ok(&self.extra_floating_digits)
        } else if name.eq_ignore_ascii_case(STATEMENT_TIMEOUT.name) {
            Ok(&self.statement_timeout)
        } else if name.eq_ignore_ascii_case(TIMEZONE.name) {
            Ok(&self.timezone)
        } else if name.eq_ignore_ascii_case(DATESTYLE.name) {
            Ok(&self.datestyle)
        } else if name.eq_ignore_ascii_case(TRANSACTION_ISOLATION.name) {
            Ok(&self.transaction_isolation)
        } else if name.eq_ignore_ascii_case(SEARCH_PATH.name) {
            Ok(&self.search_path)
        } else if name.eq_ignore_ascii_case(ENABLE_DEBUG_DATASOURCES.name) {
            Ok(&self.enable_debug_datasources)
        } else if name.eq_ignore_ascii_case(FORCE_CATALOG_REFRESH.name) {
            Ok(&self.force_catalog_refresh)
        } else if name.eq_ignore_ascii_case(GLAREDB_VERSION.name) {
            Ok(self.glaredb_version)
        } else {
            Err(ExecError::UnknownVariable(name.to_string()))
        }
    }

    /// Try to set a value for a variable.
    pub fn set(&mut self, name: &str, val: &str) -> Result<()> {
        if name.eq_ignore_ascii_case(SERVER_VERSION.name) {
            Err(ExecError::VariableReadonly(SERVER_VERSION.name.to_string()))
        } else if name.eq_ignore_ascii_case(APPLICATION_NAME.name) {
            self.application_name.set(val)
        } else if name.eq_ignore_ascii_case(CLIENT_ENCODING.name) {
            self.client_encoding.set(val)
        } else if name.eq_ignore_ascii_case(EXTRA_FLOAT_DIGITS.name) {
            self.extra_floating_digits.set(val)
        } else if name.eq_ignore_ascii_case(STATEMENT_TIMEOUT.name) {
            self.statement_timeout.set(val)
        } else if name.eq_ignore_ascii_case(TIMEZONE.name) {
            self.timezone.set(val)
        } else if name.eq_ignore_ascii_case(DATESTYLE.name) {
            self.datestyle.set(val)
        } else if name.eq_ignore_ascii_case(TRANSACTION_ISOLATION.name) {
            Err(ExecError::VariableReadonly(SERVER_VERSION.name.to_string()))
        } else if name.eq_ignore_ascii_case(SEARCH_PATH.name) {
            self.search_path.set(val)
        } else if name.eq_ignore_ascii_case(ENABLE_DEBUG_DATASOURCES.name) {
            self.enable_debug_datasources.set(val)
        } else if name.eq_ignore_ascii_case(FORCE_CATALOG_REFRESH.name) {
            self.force_catalog_refresh.set(val)
        } else if name.eq_ignore_ascii_case(GLAREDB_VERSION.name) {
            Err(ExecError::VariableReadonly(
                GLAREDB_VERSION.name.to_string(),
            ))
        } else {
            Err(ExecError::UnknownVariable(name.to_string()))
        }
    }
}

impl Default for SessionVars {
    fn default() -> Self {
        SessionVars {
            server_version: SERVER_VERSION,
            application_name: SessionVar::new(&APPLICATION_NAME),
            client_encoding: SessionVar::new(&CLIENT_ENCODING),
            extra_floating_digits: SessionVar::new(&EXTRA_FLOAT_DIGITS),
            statement_timeout: SessionVar::new(&STATEMENT_TIMEOUT),
            timezone: SessionVar::new(&TIMEZONE),
            datestyle: SessionVar::new(&DATESTYLE),
            transaction_isolation: TRANSACTION_ISOLATION,
            search_path: SessionVar::new(&SEARCH_PATH),
            enable_debug_datasources: SessionVar::new(&ENABLE_DEBUG_DATASOURCES),
            force_catalog_refresh: SessionVar::new(&FORCE_CATALOG_REFRESH),
            glaredb_version: &GLAREDB_VERSION,
        }
    }
}

pub trait AnyVar {
    /// Return the name of the varaible.
    fn name(&self) -> &'static str;

    /// Return the stringified value for the variable.
    fn formatted_value(&self) -> String;

    /// Create a record batch containg one row with the string value.
    fn record_batch(&self) -> RecordBatch {
        let val = self.formatted_value();
        let arr = StringArray::from(vec![Some(val.as_str())]);
        let schema = Schema::new(vec![Field::new(self.name(), DataType::Utf8, false)]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(arr)]).unwrap()
    }
}

/// Static configuration variables. These are all defined in code and are not
/// persisted.
#[derive(Debug)]
pub struct ServerVar<T: Value + ?Sized + 'static> {
    name: &'static str,
    value: &'static T,
}

impl<T: Value + ?Sized + 'static> ServerVar<T> {
    /// Get a reference to underlying value.
    pub fn value(&self) -> &T {
        self.value
    }
}

impl<T: Value + ?Sized + 'static> AnyVar for ServerVar<T> {
    fn name(&self) -> &'static str {
        self.name
    }

    fn formatted_value(&self) -> String {
        self.value.format()
    }
}

/// Session local variables. Unset variables will inherit the system variable.
#[derive(Debug)]
pub struct SessionVar<T: Value + ?Sized + 'static> {
    inherit: &'static ServerVar<T>,
    value: Option<T::Owned>,
}

impl<T: Value + ?Sized + 'static> SessionVar<T> {
    pub fn new(inherit: &'static ServerVar<T>) -> Self {
        SessionVar {
            inherit,
            value: None,
        }
    }

    /// Get a reference to the underlying value. If value hasn't been set for
    /// the session, the system value will be returned.
    pub fn value(&self) -> &T {
        match self.value.as_ref() {
            Some(v) => v.borrow(),
            None => self.inherit.value(),
        }
    }

    /// Set a value for this variable.
    fn set(&mut self, s: &str) -> Result<()> {
        match T::try_parse(s) {
            Some(v) => self.value = Some(v),
            None => {
                return Err(ExecError::InvalidSessionVarValue {
                    name: self.name().to_string(),
                    val: s.to_string(),
                })
            }
        }
        Ok(())
    }
}

impl<T: Value + ?Sized + 'static> AnyVar for SessionVar<T> {
    fn name(&self) -> &'static str {
        self.inherit.name()
    }

    fn formatted_value(&self) -> String {
        match &self.value {
            Some(v) => v.borrow().format(),
            None => self.inherit.formatted_value(),
        }
    }
}

pub trait Value: ToOwned {
    fn try_parse(s: &str) -> Option<Self::Owned>;
    fn format(&self) -> String;
}

impl Value for str {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(s.to_string())
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for bool {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        match s {
            "t" | "true" => Some(true),
            "f" | "false" => Some(false),
            _ => None,
        }
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

impl Value for i32 {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        s.parse().ok()
    }

    fn format(&self) -> String {
        self.to_string()
    }
}

/// Regex for matching strings delineated by commas. Will match full quoted
/// strings as well.
///
/// Taken from <https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes>
const SPLIT_ON_UNQUOTED_COMMAS: &str = r#""[^"]*"|[^,]+"#;

static COMMA_RE: Lazy<Regex> = Lazy::new(|| Regex::new(SPLIT_ON_UNQUOTED_COMMAS).unwrap());

/// Split a string on commas, preserving quoted strings.
fn split_comma_delimited(text: &str) -> Vec<String> {
    COMMA_RE
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        .collect()
}

impl Value for [String] {
    fn try_parse(s: &str) -> Option<Self::Owned> {
        Some(split_comma_delimited(s))
    }

    fn format(&self) -> String {
        self.join(",")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_on_commas() {
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
            let out = split_comma_delimited(test.input);
            assert_eq!(test.expected, out);
        }
    }
}
