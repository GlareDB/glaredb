//! String functions.

mod trim;
pub use trim::*;

mod pad;
pub use pad::*;

mod ascii;
pub use ascii::*;

mod case;
pub use case::*;

mod repeat;
pub use repeat::*;

mod substring;
pub use substring::*;

mod starts_with;
pub use starts_with::*;

mod ends_with;
pub use ends_with::*;

mod contains;
pub use contains::*;

mod length;
pub use length::*;

mod regexp_replace;
pub use regexp_replace::*;
