use std::fmt;

use glaredb_error::Result;
use glaredb_proto::ProtoConv;
use serde::{Deserialize, Serialize};

use crate::functions::cast::format::{Formatter, IntervalFormatter};

/// A representation of an interval with nanosecond resolution.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Interval {
    pub months: i32,
    pub days: i32,
    pub nanos: i64,
}

impl Interval {
    pub const ASSUMED_DAYS_IN_MONTH: i32 = 30; // Matches Postgres
    pub const ASSUMED_HOURS_IN_DAY: i32 = 24; // Matches Postgres

    pub const NANOSECONDS_IN_MICROSECOND: i64 = 1_000;
    pub const NANOSECONDS_IN_MILLISECOND: i64 = 1_000_000;
    pub const NANOSECONDS_IN_SECOND: i64 = 1_000_000_000;
    pub const NANOSECONDS_IN_MINUTE: i64 = 60 * Self::NANOSECONDS_IN_SECOND;
    pub const NANOSECONDS_IN_HOUR: i64 = 60 * Self::NANOSECONDS_IN_MINUTE;
    pub const NANOSECONDS_IN_DAY: i64 =
        Self::ASSUMED_HOURS_IN_DAY as i64 * Self::NANOSECONDS_IN_HOUR;

    pub const fn new(months: i32, days: i32, nanos: i64) -> Self {
        Interval {
            months,
            days,
            nanos,
        }
    }

    pub fn add_microseconds(&mut self, microseconds: i64) {
        self.nanos += microseconds * Self::NANOSECONDS_IN_MICROSECOND
    }

    pub fn add_milliseconds(&mut self, milliseconds: i64) {
        self.nanos += milliseconds * Self::NANOSECONDS_IN_MILLISECOND
    }

    pub fn add_seconds(&mut self, seconds: i64) {
        self.nanos += seconds * Self::NANOSECONDS_IN_SECOND
    }

    pub fn add_minutes(&mut self, minutes: i64) {
        self.nanos += minutes * Self::NANOSECONDS_IN_MINUTE
    }

    pub fn add_hours(&mut self, hours: i64) {
        self.nanos += hours * Self::NANOSECONDS_IN_HOUR
    }

    pub fn add_days(&mut self, days: i32) {
        self.days += days
    }

    pub fn add_weeks(&mut self, weeks: i32) {
        self.days += weeks * 7
    }

    pub fn add_months(&mut self, months: i32) {
        self.months += months
    }

    pub fn add_years(&mut self, years: i32) {
        self.months += years * 12
    }

    pub fn add_decades(&mut self, decades: i32) {
        self.add_years(decades * 10)
    }

    pub fn add_centuries(&mut self, centuries: i32) {
        self.add_years(centuries * 100)
    }

    pub fn add_millenium(&mut self, millenium: i32) {
        self.add_years(millenium * 1000)
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        IntervalFormatter.write(self, f)
    }
}

impl ProtoConv for Interval {
    type ProtoType = glaredb_proto::generated::expr::IntervalScalar;

    fn to_proto(&self) -> Result<Self::ProtoType> {
        Ok(Self::ProtoType {
            months: self.months,
            days: self.days,
            nanos: self.nanos,
        })
    }

    fn from_proto(proto: Self::ProtoType) -> Result<Self> {
        Ok(Self {
            months: proto.months,
            days: proto.days,
            nanos: proto.nanos,
        })
    }
}
