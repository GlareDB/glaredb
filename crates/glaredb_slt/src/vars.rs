use std::collections::HashMap;
use std::fmt;

use rand::distributions::Alphanumeric;
use rand::Rng;

#[derive(Clone)]
pub enum VarValue {
    /// Value is sensitive, don't print it out.
    Sensitive(String),
    /// Value is not sensitive, print it during debugging.
    Plain(String),
}

impl VarValue {
    pub fn plain_from_env(key: &str) -> VarValue {
        match std::env::var(key) {
            Ok(s) => VarValue::Plain(s),
            Err(_) => {
                println!("Missing environment variable: {key}");
                std::process::exit(2);
            }
        }
    }

    pub fn sensitive_from_env(key: &str) -> VarValue {
        match std::env::var(key) {
            Ok(s) => VarValue::Sensitive(s),
            Err(_) => {
                println!("Missing environment variable: {key}");
                std::process::exit(2);
            }
        }
    }
}

impl fmt::Display for VarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sensitive(_) => write!(f, "***"),
            VarValue::Plain(s) => write!(f, "{s}"),
        }
    }
}

impl fmt::Debug for VarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl AsRef<str> for VarValue {
    fn as_ref(&self) -> &str {
        match self {
            Self::Sensitive(s) => s.as_str(),
            VarValue::Plain(s) => s.as_str(),
        }
    }
}

/// Variables that can be referenced in sql queries and automatically replaced
/// with concrete values.
///
/// Variable format in sql queries: __MYVARIABLE__
///
/// When adding a variable, they'll automatically be uppercased and surrounded
/// with understores.
///
/// See default implementation for predefined variables.
///
/// A new instance of these variables is created for each file run.
#[derive(Debug, Clone)]
pub struct ReplacementVars {
    vars: HashMap<String, VarValue>,
}

impl Default for ReplacementVars {
    fn default() -> Self {
        let mut vars = ReplacementVars {
            vars: HashMap::new(),
        };

        let s: String = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(7)
            .map(char::from)
            .collect();

        // Dir (relative to test_bin) where tests can write temp files for
        // things like COPY TO.
        vars.add_var("SLT_TMP", VarValue::Plain(format!("../slt_tmp/{s}")));

        vars
    }
}

impl ReplacementVars {
    pub fn add_var(&mut self, key: &str, val: VarValue) {
        let key = format!("__{}__", key.to_uppercase());
        self.vars.insert(key, val);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &VarValue)> {
        self.vars.iter()
    }
}

impl fmt::Display for ReplacementVars {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (k, v) in &self.vars {
            writeln!(f, "{k} = {v}")?;
        }
        Ok(())
    }
}
