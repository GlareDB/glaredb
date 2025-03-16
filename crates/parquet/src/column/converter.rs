#![allow(unused)]

use std::fmt::Debug;

use rayexec_execution::util::marker::PhantomCovariant;

/// Describes converting a raw parquet value to to an internal type.
pub trait ValueConverter: Debug {
    /// The parquet input type.
    type Input: Sized;
    /// The physcial output type.
    type Output: Sized;

    /// Convert from an input value to an output value.
    ///
    /// This should never fail.
    fn convert(input: Self::Input) -> Self::Output;
}

/// A converter that returns its inputs unchanged.
#[derive(Debug)]
pub struct NopConverter<T> {
    _t: PhantomCovariant<T>,
}

impl<T> NopConverter<T> {
    pub const fn new() -> Self {
        NopConverter {
            _t: PhantomCovariant::new(),
        }
    }
}

impl<T> ValueConverter for NopConverter<T>
where
    T: Debug,
{
    type Input = T;
    type Output = T;

    fn convert(input: Self::Input) -> Self::Output {
        input
    }
}
