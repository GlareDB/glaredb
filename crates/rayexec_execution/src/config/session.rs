use std::collections::HashMap;
use std::sync::LazyLock;

use rayexec_error::{RayexecError, Result};

use crate::arrays::scalar::{OwnedScalarValue, ScalarValue};
use crate::runtime::{PipelineExecutor, Runtime};

/// Configuration for the session.
#[derive(Debug)]
pub struct SessionConfig {
    pub enable_optimizer: bool,
    pub application_name: String,
    pub allow_nested_loop_join: bool,
    pub partitions: u64,
    pub batch_size: u64,
    pub verify_optimized_plan: bool,
    pub enable_function_chaining: bool,
}

impl SessionConfig {
    pub fn new<P, R>(executor: &P, _runtime: &R) -> Self
    where
        P: PipelineExecutor,
        R: Runtime,
    {
        SessionConfig {
            enable_optimizer: true,
            application_name: String::new(),
            allow_nested_loop_join: true,
            partitions: executor.default_partitions() as u64,
            batch_size: 4096,
            verify_optimized_plan: false,
            enable_function_chaining: true,
        }
    }

    pub fn set_from_scalar(&mut self, name: &str, value: ScalarValue) -> Result<()> {
        let func = GET_SET_FUNCTIONS
            .get(name)
            .ok_or_else(|| RayexecError::new("Missing setting for '{name}'"))?;

        (func.set)(value, self)
    }

    pub fn get_as_scalar(&self, name: &str) -> Result<OwnedScalarValue> {
        let func = GET_SET_FUNCTIONS
            .get(name)
            .ok_or_else(|| RayexecError::new("Missing setting for '{name}'"))?;

        let val = (func.get)(self);
        Ok(val)
    }

    pub fn reset<P, R>(&mut self, name: &str, executor: &P, runtime: &R) -> Result<()>
    where
        P: PipelineExecutor,
        R: Runtime,
    {
        // TODO: I don't hate it, but could be more efficient.
        let def_conf = Self::new(executor, runtime);

        let func = GET_SET_FUNCTIONS
            .get(name)
            .ok_or_else(|| RayexecError::new("Missing setting for '{name}'"))?;

        let scalar = (func.get)(&def_conf);
        (func.set)(scalar, self)
    }

    pub fn reset_all<P, R>(&mut self, executor: &P, runtime: &R)
    where
        P: PipelineExecutor,
        R: Runtime,
    {
        *self = Self::new(executor, runtime);
    }
}

struct SettingFunctions {
    set: fn(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()>,
    get: fn(conf: &SessionConfig) -> OwnedScalarValue,
}

impl SettingFunctions {
    const fn new<S: SessionSetting>() -> Self {
        SettingFunctions {
            set: S::set_from_scalar as _,
            get: S::get_as_scalar as _,
        }
    }
}

fn insert_setting<S: SessionSetting>(map: &mut HashMap<&'static str, SettingFunctions>) {
    if map.insert(S::NAME, SettingFunctions::new::<S>()).is_some() {
        panic!("Duplicate settings names: {}", S::NAME);
    }
}

static GET_SET_FUNCTIONS: LazyLock<HashMap<&'static str, SettingFunctions>> = LazyLock::new(|| {
    let mut map = HashMap::new();

    insert_setting::<EnableOptimizer>(&mut map);
    insert_setting::<ApplicationName>(&mut map);
    insert_setting::<AllowNestedLoopJoin>(&mut map);
    insert_setting::<Partitions>(&mut map);
    insert_setting::<BatchSize>(&mut map);
    insert_setting::<EnableFunctionChaining>(&mut map);

    map
});

pub trait SessionSetting: Sync + Send + 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()>;
    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue;
}

pub struct EnableOptimizer;

impl SessionSetting for EnableOptimizer {
    const NAME: &'static str = "enable_optimizer";
    const DESCRIPTION: &'static str = "Controls if the optimizer is enabled";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.enable_optimizer = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.enable_optimizer.into()
    }
}

pub struct ApplicationName;

impl SessionSetting for ApplicationName {
    const NAME: &'static str = "application_name";
    const DESCRIPTION: &'static str = "Postgres compatability variable";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_into_string()?;
        conf.application_name = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.application_name.clone().into()
    }
}

pub struct AllowNestedLoopJoin;

impl SessionSetting for AllowNestedLoopJoin {
    const NAME: &'static str = "allow_nested_loop_join";
    const DESCRIPTION: &'static str = "If nested loop join operators are allowed in the plan";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.allow_nested_loop_join = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.allow_nested_loop_join.into()
    }
}

pub struct Partitions;

impl SessionSetting for Partitions {
    const NAME: &'static str = "partitions";
    const DESCRIPTION: &'static str = "Number of partitions to use during execution";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_i64()?;
        conf.partitions = val as u64;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.partitions.into()
    }
}

pub struct BatchSize;

impl SessionSetting for BatchSize {
    const NAME: &'static str = "batch_size";
    const DESCRIPTION: &'static str = "Desired number of rows in a batch";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_i64()?;
        conf.batch_size = val as u64;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.batch_size.into()
    }
}

pub struct VerifyOptimizedPlan;

impl SessionSetting for VerifyOptimizedPlan {
    const NAME: &'static str = "verify_optimized_plan";
    const DESCRIPTION: &'static str =
        "Compare results of the optimized plan with the results from the unoptimized plan";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.verify_optimized_plan = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.verify_optimized_plan.into()
    }
}

pub struct EnableFunctionChaining;

impl SessionSetting for EnableFunctionChaining {
    const NAME: &'static str = "enable_function_chaining";
    const DESCRIPTION: &'static str = "If function chaining syntax is enabled.";

    fn set_from_scalar(scalar: ScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.enable_function_chaining = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> OwnedScalarValue {
        conf.enable_function_chaining.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_test_config() -> SessionConfig {
        SessionConfig {
            enable_optimizer: true,
            application_name: String::new(),
            allow_nested_loop_join: true,
            partitions: 8,
            batch_size: 4096,
            verify_optimized_plan: false,
            enable_function_chaining: true,
        }
    }

    #[test]
    fn set_setting_exists() {
        let mut conf = new_test_config();
        conf.set_from_scalar("application_name", "test".into())
            .unwrap();

        let val = conf.get_as_scalar("application_name").unwrap();
        assert_eq!("test", val.try_as_str().unwrap());
    }

    #[test]
    fn set_setting_not_exists() {
        let mut conf = new_test_config();
        conf.set_from_scalar("hell_world", 58.into()).unwrap_err();
    }

    #[test]
    fn set_casts_value() {
        let mut conf = new_test_config();
        conf.set_from_scalar("partitions", ScalarValue::Int8(13))
            .unwrap();

        let val = conf.get_as_scalar("partitions").unwrap();
        assert_eq!(ScalarValue::UInt64(13), val);
    }
}
