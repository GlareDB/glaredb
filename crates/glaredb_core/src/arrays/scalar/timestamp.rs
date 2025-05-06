use serde::{Deserialize, Serialize};

use crate::arrays::datatype::TimeUnit;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Hash)]
pub struct TimestampScalar {
    pub unit: TimeUnit,
    pub value: i64,
}
