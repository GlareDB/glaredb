use crate::expr::scalar::ScalarValue;
use once_cell::sync::Lazy;
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;

static DEFAULT_GLOBAL_SESSION_VARS: Lazy<SessionVars> = Lazy::new(|| SessionVars::global_default());

#[derive(Debug, Clone, PartialEq)]
pub struct SessionVar {
    pub name: &'static str,
    pub value: ScalarValue,
}

impl SessionVar {
    /// Validate that the type of the provided value is valid for this variable.
    pub fn validate_type(&self, value: &ScalarValue) -> Result<()> {
        if self.value.data_type() != value.data_type() {
            return Err(RayexecError::new(format!(
                "Invalid value for session variable {}, expected a value of type {}",
                self.name,
                self.value.data_type()
            )));
        }

        Ok(())
    }
}

/// Session local variables.
#[derive(Debug, Clone)]
pub struct SessionVars {
    /// Explicitly set variables for the session.
    ///
    /// This is really an overlay on top of the global defaults and provides
    /// COW-like semantics for variables.
    vars: HashMap<&'static str, SessionVar>,
}

impl SessionVars {
    /// Create a session variables map that contains the default values.
    fn global_default() -> Self {
        let vars = [
            SessionVar {
                name: "debug_string_var",
                value: ScalarValue::Utf8("debug".to_string()),
            },
            SessionVar {
                name: "debug_error_on_nested_loop_join",
                value: ScalarValue::Boolean(false),
            },
        ];

        let vars = vars.into_iter().map(|var| (var.name, var)).collect();

        SessionVars { vars }
    }

    /// Creates session local variables.
    pub fn new_local() -> Self {
        SessionVars {
            vars: HashMap::new(),
        }
    }

    pub fn get_var(&self, name: &str) -> Result<&SessionVar> {
        if let Some(var) = self.vars.get(name) {
            return Ok(var);
        }

        if let Some(var) = DEFAULT_GLOBAL_SESSION_VARS.vars.get(name) {
            return Ok(var);
        }

        Err(RayexecError::new(format!(
            "Session variable doesn't exist: {name}"
        )))
    }

    pub fn exists(&self, name: &str) -> bool {
        self.get_var(name).is_ok()
    }

    pub fn set_var(&mut self, name: &str, value: ScalarValue) -> Result<()> {
        if let Some(var) = self.vars.get_mut(name) {
            var.validate_type(&value)?;

            var.value = value;
            return Ok(());
        }

        // Only allow setting variables that we've actually defined and make
        // sure they're the right type.
        if let Some(global) = DEFAULT_GLOBAL_SESSION_VARS.vars.get(name) {
            global.validate_type(&value)?;

            let mut var = global.clone();
            var.value = value;
            self.vars.insert(var.name, var);
            return Ok(());
        }

        Err(RayexecError::new(format!(
            "Session variable doesn't exist: {name}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_var_exists() {
        let mut vars = SessionVars::new_local();
        let var = vars.get_var("debug_string_var").unwrap();
        assert_eq!(ScalarValue::Utf8("debug".to_string()), var.value);

        vars.set_var("debug_string_var", ScalarValue::Utf8("test".to_string()))
            .unwrap();
        let var = vars.get_var("debug_string_var").unwrap();
        assert_eq!(ScalarValue::Utf8("test".to_string()), var.value);
    }

    #[test]
    fn set_var_wrong_type() {
        let mut vars = SessionVars::new_local();
        vars.set_var("debug_string_var", ScalarValue::Int8(1))
            .unwrap_err();
    }

    #[test]
    fn set_var_not_exists() {
        let mut vars = SessionVars::new_local();
        vars.set_var("does_not_exist", ScalarValue::Utf8("test".to_string()))
            .unwrap_err();
    }
}
