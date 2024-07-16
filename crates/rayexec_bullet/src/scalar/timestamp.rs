use serde::{Deserialize, Serialize};

use crate::datatype::TimeUnit;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TimestampScalar {
    pub unit: TimeUnit,
    pub value: i64,
}
