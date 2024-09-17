use once_cell::sync::Lazy;
use rayexec_bullet::{
    compute::cast::scalar::cast_scalar,
    scalar::{OwnedScalarValue, ScalarValue},
};
use rayexec_error::{RayexecError, Result};
use std::collections::HashMap;

use super::{ExecutablePlanConfig, IntermediatePlanConfig};

static DEFAULT_GLOBAL_SESSION_VARS: Lazy<SessionVars> = Lazy::new(SessionVars::global_default);

#[derive(Debug, Clone, PartialEq)]
pub struct SessionVar {
    pub name: &'static str,
    pub desc: &'static str,
    pub value: OwnedScalarValue,
}

impl SessionVar {
    /// Validate that the type of the provided value is valid for this variable.
    pub fn validate_type(&self, value: &ScalarValue) -> Result<()> {
        if self.value.datatype() != value.datatype() {
            return Err(RayexecError::new(format!(
                "Invalid value for session variable {}, expected a value of type {}",
                self.name,
                self.value.datatype()
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
        let default_vars = [
            SessionVar {
                name: "debug_string_var",
                desc: "Debug variable for testing SET/SHOW.",
                value: ScalarValue::Utf8("debug".into()),
            },
            SessionVar {
                name: "application_name",
                desc: "Postgres compatability variable.",
                value: ScalarValue::Utf8("".into()),
            },
            SessionVar {
                name: "allow_nested_loop_join",
                desc: "If nested loop join operators are allowed in the plan.",
                value: ScalarValue::Boolean(true),
            },
            SessionVar {
                name: "partitions",
                desc: "Number of partitions to use during execution.",
                value: ScalarValue::UInt64(num_cpus::get() as u64),
            },
            SessionVar {
                name: "batch_size",
                desc: "Desired number of rows in a batch.",
                value: ScalarValue::UInt64(2048),
            },
        ];

        let mut vars = HashMap::new();
        for var in default_vars {
            if let Some(existing) = vars.insert(var.name, var) {
                panic!("duplicate vars: {}", existing.name);
            }
        }

        SessionVars { vars }
    }

    /// Creates session local variables.
    pub fn new_local() -> Self {
        SessionVars {
            vars: HashMap::new(),
        }
    }

    pub fn intermediate_plan_config(&self) -> IntermediatePlanConfig {
        IntermediatePlanConfig {
            allow_nested_loop_join: self
                .get_var_expect("allow_nested_loop_join")
                .value
                .try_as_bool()
                .expect("variable to be bool"),
        }
    }

    pub fn executable_plan_config(&self) -> ExecutablePlanConfig {
        ExecutablePlanConfig {
            partitions: self
                .get_var_expect("partitions")
                .value
                .try_as_usize()
                .expect("convertable to usize"),
        }
    }

    /// Get a session variable, erroring if the variable doens't exist.
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

    /// Get a session variable, panicking if it doesn't exist.
    pub fn get_var_expect(&self, name: &str) -> &SessionVar {
        self.get_var(name).expect("variable to exist")
    }

    pub fn exists(&self, name: &str) -> bool {
        self.get_var(name).is_ok()
    }

    /// Try to cast a scalar value to the correct type for a variable if needed.
    pub fn try_cast_scalar_value(
        &self,
        name: &str,
        value: OwnedScalarValue,
    ) -> Result<OwnedScalarValue> {
        let var = DEFAULT_GLOBAL_SESSION_VARS
            .vars
            .get(name)
            .ok_or_else(|| RayexecError::new(format!("Session variable doesn't exist: {name}")))?;
        let value = cast_scalar(value, &var.value.datatype())?;

        Ok(value)
    }

    pub fn set_var(&mut self, name: &str, value: OwnedScalarValue) -> Result<()> {
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

    pub fn reset_var(&mut self, name: &str) -> Result<()> {
        if !self.exists(name) {
            return Err(RayexecError::new(format!(
                "Session variable doesn't exist: {name}"
            )));
        }

        // Doesn't matter if the user has actully set the variable previously.
        let _ = self.vars.remove(name);

        Ok(())
    }

    pub fn reset_all(&mut self) {
        self.vars.clear()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_var_names() {
        // Panics on non-unique names.
        let _ = SessionVars::global_default();
    }

    #[test]
    fn set_var_exists() {
        let mut vars = SessionVars::new_local();
        let var = vars.get_var("debug_string_var").unwrap();
        assert_eq!(ScalarValue::Utf8("debug".into()), var.value);

        vars.set_var("debug_string_var", ScalarValue::Utf8("test".into()))
            .unwrap();
        let var = vars.get_var("debug_string_var").unwrap();
        assert_eq!(ScalarValue::Utf8("test".into()), var.value);
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
        vars.set_var("does_not_exist", ScalarValue::Utf8("test".into()))
            .unwrap_err();
    }

    #[test]
    fn cast_value() {
        let vars = SessionVars::new_local();
        let new_value = ScalarValue::Int64(8096);
        let new_value = vars.try_cast_scalar_value("batch_size", new_value).unwrap();
        assert_eq!(ScalarValue::UInt64(8096), new_value);
    }
}
