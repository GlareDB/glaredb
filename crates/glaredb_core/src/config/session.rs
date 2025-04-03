use std::collections::HashMap;
use std::sync::LazyLock;

use glaredb_error::{DbError, Result};

use crate::arrays::scalar::{BorrowedScalarValue, ScalarValue};
use crate::runtime::io::IoRuntime;
use crate::runtime::pipeline::PipelineRuntime;

/// Configuration for the session.
#[derive(Debug)]
pub struct SessionConfig {
    pub enable_optimizer: bool,
    pub application_name: String,
    pub partitions: u64,
    pub batch_size: u64,
    pub verify_optimized_plan: bool,
    pub enable_function_chaining: bool,
    pub per_partition_counts: bool,
}

impl SessionConfig {
    pub fn new<P, R>(executor: &P, _runtime: &R) -> Self
    where
        P: PipelineRuntime,
        R: IoRuntime,
    {
        SessionConfig {
            enable_optimizer: true,
            application_name: String::new(),
            partitions: executor.default_partitions() as u64,
            batch_size: DEFAULT_BATCH_SIZE as u64,
            verify_optimized_plan: false,
            enable_function_chaining: true,
            per_partition_counts: false,
        }
    }

    pub fn set_from_scalar(&mut self, name: &str, value: BorrowedScalarValue) -> Result<()> {
        let func = GET_SET_FUNCTIONS
            .get(name)
            .ok_or_else(|| DbError::new(format!("Missing setting for '{name}'")))?;

        (func.set)(value, self)
    }

    pub fn get_as_scalar(&self, name: &str) -> Result<ScalarValue> {
        let func = GET_SET_FUNCTIONS
            .get(name)
            .ok_or_else(|| DbError::new(format!("Missing setting for '{name}'")))?;

        let val = (func.get)(self);
        Ok(val)
    }

    pub fn reset<P, R>(&mut self, name: &str, executor: &P, runtime: &R) -> Result<()>
    where
        P: PipelineRuntime,
        R: IoRuntime,
    {
        // TODO: I don't hate it, but could be more efficient.
        let def_conf = Self::new(executor, runtime);

        let func = GET_SET_FUNCTIONS
            .get(name)
            .ok_or_else(|| DbError::new(format!("Missing setting for '{name}'")))?;

        let scalar = (func.get)(&def_conf);
        (func.set)(scalar, self)
    }

    pub fn reset_all<P, R>(&mut self, executor: &P, runtime: &R)
    where
        P: PipelineRuntime,
        R: IoRuntime,
    {
        *self = Self::new(executor, runtime);
    }
}

struct SettingFunctions {
    set: fn(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()>,
    get: fn(conf: &SessionConfig) -> ScalarValue,
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
    insert_setting::<Partitions>(&mut map);
    insert_setting::<BatchSize>(&mut map);
    insert_setting::<VerifyOptimizedPlan>(&mut map);
    insert_setting::<EnableFunctionChaining>(&mut map);
    insert_setting::<PerPartitionCounts>(&mut map);

    map
});

pub trait SessionSetting: Sync + Send + 'static {
    const NAME: &'static str;
    const DESCRIPTION: &'static str;

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()>;
    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue;
}

pub struct EnableOptimizer;

impl SessionSetting for EnableOptimizer {
    const NAME: &'static str = "enable_optimizer";
    const DESCRIPTION: &'static str = "Controls if the optimizer is enabled";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.enable_optimizer = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.enable_optimizer.into()
    }
}

pub struct ApplicationName;

impl SessionSetting for ApplicationName {
    const NAME: &'static str = "application_name";
    const DESCRIPTION: &'static str = "Postgres compatability variable";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_into_string()?;
        conf.application_name = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.application_name.clone().into()
    }
}

const MIN_PARTITION_COUNT: usize = 1;
const MAX_PARTITION_COUNT: usize = 512;

pub struct Partitions;

impl Partitions {
    pub fn validate_value(val: usize) -> Result<()> {
        if val < MIN_PARTITION_COUNT {
            return Err(DbError::new(format!(
                "Partition count cannot be less than {MIN_PARTITION_COUNT}"
            )));
        }

        if val > MAX_PARTITION_COUNT {
            return Err(DbError::new(format!(
                "Partition count cannot be greater than {MAX_PARTITION_COUNT}"
            )));
        }

        Ok(())
    }
}

impl SessionSetting for Partitions {
    const NAME: &'static str = "partitions";
    const DESCRIPTION: &'static str = "Number of partitions to use during execution";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_usize()?;
        Self::validate_value(val)?;

        conf.partitions = val as u64;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.partitions.into()
    }
}

pub const DEFAULT_BATCH_SIZE: usize = 2048;

const MIN_BATCH_SIZE: usize = 1;
const MAX_BATCH_SIZE: usize = 8192;

pub struct BatchSize;

impl SessionSetting for BatchSize {
    const NAME: &'static str = "batch_size";
    const DESCRIPTION: &'static str = "Desired number of rows in a batch";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_usize()?;

        if val < MIN_BATCH_SIZE {
            return Err(DbError::new(format!(
                "Batch size cannot be less than {MIN_BATCH_SIZE}"
            )));
        }

        if val > MAX_BATCH_SIZE {
            return Err(DbError::new(format!(
                "Batch size cannot be greater than {MAX_BATCH_SIZE}"
            )));
        }

        conf.batch_size = val as u64;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.batch_size.into()
    }
}

pub struct VerifyOptimizedPlan;

impl SessionSetting for VerifyOptimizedPlan {
    const NAME: &'static str = "verify_optimized_plan";
    const DESCRIPTION: &'static str =
        "Compare results of the optimized plan with the results from the unoptimized plan";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.verify_optimized_plan = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.verify_optimized_plan.into()
    }
}

pub struct EnableFunctionChaining;

impl SessionSetting for EnableFunctionChaining {
    const NAME: &'static str = "enable_function_chaining";
    const DESCRIPTION: &'static str = "If function chaining syntax is enabled.";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.enable_function_chaining = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.enable_function_chaining.into()
    }
}

pub struct PerPartitionCounts;

impl SessionSetting for PerPartitionCounts {
    const NAME: &'static str = "per_partition_counts";
    const DESCRIPTION: &'static str =
        "Show insert row totals per-partition instead of as a total count.";

    fn set_from_scalar(scalar: BorrowedScalarValue, conf: &mut SessionConfig) -> Result<()> {
        let val = scalar.try_as_bool()?;
        conf.per_partition_counts = val;
        Ok(())
    }

    fn get_as_scalar(conf: &SessionConfig) -> ScalarValue {
        conf.per_partition_counts.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn new_test_config() -> SessionConfig {
        SessionConfig {
            enable_optimizer: true,
            application_name: String::new(),
            partitions: 8,
            batch_size: 4096,
            verify_optimized_plan: false,
            enable_function_chaining: true,
            per_partition_counts: false,
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
        conf.set_from_scalar("partitions", BorrowedScalarValue::Int8(13))
            .unwrap();

        let val = conf.get_as_scalar("partitions").unwrap();
        assert_eq!(BorrowedScalarValue::UInt64(13), val);
    }
}
