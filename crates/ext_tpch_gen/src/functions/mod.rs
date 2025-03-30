mod region;
pub use region::*;

mod part;
pub use part::*;

mod supplier;
pub use supplier::*;

mod customer;
pub use customer::*;

mod partsupp;
pub use partsupp::*;

mod orders;
pub use orders::*;

mod lineitem;
pub use lineitem::*;

mod nation;
pub use nation::*;

mod table_gen;

pub(crate) mod convert {
    use tpchgen::dates::TPCHDate;
    use tpchgen::generators::OrderStatus;

    /// Number of days after 1970-01-01 to represent the min TPCH date
    /// (1992-01-01).
    const MIN_DATE_DAYS_AFTER_EPOCH: i32 = 8302;

    /// Convert a tpch date to number of days after unix epoch.
    pub fn tpch_date_to_days_after_epoch(date: TPCHDate) -> i32 {
        date.into_inner() + MIN_DATE_DAYS_AFTER_EPOCH
    }

    // TODO: Remove
    // <https://github.com/clflushopt/tpchgen-rs/pull/95>
    pub const fn status_to_str(status: OrderStatus) -> &'static str {
        match status {
            OrderStatus::Fulfilled => "F",
            OrderStatus::Open => "O",
            OrderStatus::Pending => "P",
        }
    }
}
