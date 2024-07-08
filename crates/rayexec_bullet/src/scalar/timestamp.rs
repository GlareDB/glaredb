use crate::datatype::TimeUnit;

#[derive(Debug, Clone, PartialEq)]
pub struct TimestampScalar {
    pub unit: TimeUnit,
    pub value: i64,
}
