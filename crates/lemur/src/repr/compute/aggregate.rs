use super::{
    value_vec_dispatch_unary,
    value_vec_dispatch_unary_groups, NumericType,
};
use crate::repr::sort::GroupRanges;
use crate::repr::value::ValueVec;
use crate::repr::vec::{BoolVec, FixedLengthType, FixedLengthVec, Int32Vec, Utf8Vec};
use anyhow::{anyhow, Result};
use bitvec::vec::BitVec;

/// Aggregate using some `reduce` function on a fixed length vector.
///
/// The first non-null value is passed to `init`. Each subsequent non-null value
/// will be passed to `reduce` along with the accumulated value.
fn agg_fixedlen_init_first<T, O, F1, F2>(
    vec: &FixedLengthVec<T>,
    init: F1,
    reduce: F2,
) -> FixedLengthVec<O>
where
    T: FixedLengthType,
    O: FixedLengthType,
    F1: Fn(T) -> O,
    F2: Fn(O, T) -> O,
{
    let mut acc = None;
    for val in vec.iter().flatten() {
        match acc {
            Some(inner) => acc = Some(reduce(inner, *val)),
            None => acc = Some(init(*val)),
        }
    }
    FixedLengthVec::one(acc)
}

/// Compute aggregates for each group. The returned vector's length will equal
/// the number number of groups.
fn agg_fixedlen_init_first_groups<T, O, F1, F2>(
    vec: &FixedLengthVec<T>,
    groups: &GroupRanges,
    init: F1,
    reduce: F2,
) -> FixedLengthVec<O>
where
    T: FixedLengthType,
    O: FixedLengthType,
    F1: Fn(T) -> O,
    F2: Fn(O, T) -> O,
{
    let mut output = FixedLengthVec::with_capacity(groups.num_groups());
    for range in groups.iter_ranges() {
        let mut acc = None;
        let len = range.end - range.start;
        let iter = vec.iter().skip(range.start).take(len);
        for val in iter.flatten() {
            match acc {
                Some(inner) => acc = Some(reduce(inner, *val)),
                None => acc = Some(init(*val)),
            }
        }
        output.push(acc)
    }
    output
}

/// Similar to `agg_fixedlen_init_first`, but with a utf8 vector.
fn agg_utf8_init_first<F1, F2>(vec: &Utf8Vec, init: F1, reduce: F2) -> Utf8Vec
where
    F1: Fn(&str) -> String,
    F2: Fn(String, &str) -> String,
{
    // TODO: Possibly COW
    let mut acc = None;
    for val in vec.iter().flatten() {
        match acc {
            Some(inner) => acc = Some(reduce(inner, val)),
            None => acc = Some(init(val)),
        }
    }
    Utf8Vec::one(acc.as_deref())
}

fn agg_utf8_init_first_groups<F1, F2>(
    vec: &Utf8Vec,
    groups: &GroupRanges,
    init: F1,
    reduce: F2,
) -> Utf8Vec
where
    F1: Fn(&str) -> String,
    F2: Fn(String, &str) -> String,
{
    let mut output = Utf8Vec::with_capacity(groups.num_groups());
    for range in groups.iter_ranges() {
        let mut acc = None;
        let len = range.end - range.start;
        let iter = vec.iter().skip(range.start).take(len);
        for val in iter.flatten() {
            match acc {
                Some(inner) => acc = Some(reduce(inner, val)),
                None => acc = Some(init(val)),
            }
        }
        output.push(acc.as_deref())
    }
    output
}

pub trait VecCountAggregate {
    type Output;

    // TODO: Add `count_any` for counting non-null
    fn count(&self) -> Result<Self::Output> {
        Err(anyhow!("count unimplemented"))
    }

    fn count_groups(&self, _groups: &GroupRanges) -> Result<Self::Output> {
        Err(anyhow!("count groups unimplemented"))
    }
}

impl<T: FixedLengthType> VecCountAggregate for FixedLengthVec<T> {
    // TODO: Should be numeric
    type Output = Int32Vec;

    fn count(&self) -> Result<Int32Vec> {
        Ok(Int32Vec::one(Some(self.len() as i32)))
    }

    fn count_groups(&self, groups: &GroupRanges) -> Result<Int32Vec> {
        let vals: Vec<_> = groups.iter_lens().map(|len| len as i32).collect();
        let validity = BitVec::repeat(true, vals.len());
        Ok(Int32Vec::from_parts(validity, vals))
    }
}

impl VecCountAggregate for Utf8Vec {
    // TODO: Should be numeric
    type Output = Int32Vec;

    fn count(&self) -> Result<Int32Vec> {
        Ok(Int32Vec::one(Some(self.len() as i32)))
    }

    fn count_groups(&self, groups: &GroupRanges) -> Result<Int32Vec> {
        let vals: Vec<_> = groups.iter_lens().map(|len| len as i32).collect();
        let validity = BitVec::repeat(true, vals.len());
        Ok(Int32Vec::from_parts(validity, vals))
    }
}

impl VecCountAggregate for ValueVec {
    type Output = Self;

    fn count(&self) -> Result<Self> {
        Ok(Int32Vec::one(Some(self.len() as i32)).into())
    }

    fn count_groups(&self, groups: &GroupRanges) -> Result<Self> {
        let vals: Vec<_> = groups.iter_lens().map(|len| len as i32).collect();
        let validity = BitVec::repeat(true, vals.len());
        Ok(Int32Vec::from_parts(validity, vals).into())
    }
}

pub trait VecUnaryAggregate<Rhs = Self>: Sized {
    /// Return the first non-null value for each group, or null if
    /// all values in the group are null.
    fn first_groups(&self, _groups: &GroupRanges) -> Result<Self> {
        Err(anyhow!("first groups unimplemented"))
    }
}

impl<T: FixedLengthType> VecUnaryAggregate for FixedLengthVec<T> {
    fn first_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(agg_fixedlen_init_first_groups(
            self,
            groups,
            |v| v,
            |acc, _| acc,
        ))
    }
}

impl VecUnaryAggregate for Utf8Vec {
    fn first_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(agg_utf8_init_first_groups(
            self,
            groups,
            |v| v.to_string(),
            |acc, _| acc,
        ))
    }
}

impl VecUnaryAggregate for ValueVec {
    fn first_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(value_vec_dispatch_unary_groups!(
            self,
            groups,
            VecUnaryAggregate::first_groups
        ))
    }
}

pub trait VecUnaryCmpAggregate: Sized {
    fn min(&self) -> Result<Self> {
        Err(anyhow!("min unimplemented"))
    }

    fn min_groups(&self, _groups: &GroupRanges) -> Result<Self> {
        Err(anyhow!("min groups unimplemented"))
    }

    fn max(&self) -> Result<Self> {
        Err(anyhow!("max unimplemented"))
    }

    fn max_groups(&self, _groups: &GroupRanges) -> Result<Self> {
        Err(anyhow!("max groups unimplemented"))
    }
}

impl<T: FixedLengthType> VecUnaryCmpAggregate for FixedLengthVec<T> {
    fn min(&self) -> Result<Self> {
        Ok(agg_fixedlen_init_first(
            self,
            |v| v,
            |acc, v| if acc < v { acc } else { v },
        ))
    }

    fn min_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(agg_fixedlen_init_first_groups(
            self,
            groups,
            |v| v,
            |acc, v| if acc < v { acc } else { v },
        ))
    }

    fn max(&self) -> Result<Self> {
        Ok(agg_fixedlen_init_first(
            self,
            |v| v,
            |acc, v| if acc > v { acc } else { v },
        ))
    }

    fn max_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(agg_fixedlen_init_first_groups(
            self,
            groups,
            |v| v,
            |acc, v| if acc > v { acc } else { v },
        ))
    }
}

impl VecUnaryCmpAggregate for Utf8Vec {
    fn min(&self) -> Result<Self> {
        Ok(agg_utf8_init_first(
            self,
            |s| s.to_string(),
            |acc, v| {
                if acc.as_str() < v {
                    acc
                } else {
                    v.to_string() // TODO: Be efficient.
                }
            },
        ))
    }

    fn min_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(agg_utf8_init_first_groups(
            self,
            groups,
            |s| s.to_string(),
            |acc, v| {
                if acc.as_str() < v {
                    acc
                } else {
                    v.to_string() // TODO: Be efficient.
                }
            },
        ))
    }

    fn max(&self) -> Result<Self> {
        Ok(agg_utf8_init_first(
            self,
            |s| s.to_string(),
            |acc, v| {
                if acc.as_str() > v {
                    acc
                } else {
                    v.to_string() // TODO: Be efficient.
                }
            },
        ))
    }

    fn max_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(agg_utf8_init_first_groups(
            self,
            groups,
            |s| s.to_string(),
            |acc, v| {
                if acc.as_str() > v {
                    acc
                } else {
                    v.to_string() // TODO: Be efficient.
                }
            },
        ))
    }
}

impl VecUnaryCmpAggregate for ValueVec {
    fn min(&self) -> Result<Self> {
        Ok(value_vec_dispatch_unary!(self, VecUnaryCmpAggregate::min))
    }

    fn min_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(value_vec_dispatch_unary_groups!(
            self,
            groups,
            VecUnaryCmpAggregate::min_groups
        ))
    }

    fn max(&self) -> Result<Self> {
        Ok(value_vec_dispatch_unary!(self, VecUnaryCmpAggregate::max))
    }

    fn max_groups(&self, groups: &GroupRanges) -> Result<Self> {
        Ok(value_vec_dispatch_unary_groups!(
            self,
            groups,
            VecUnaryCmpAggregate::max_groups
        ))
    }
}

pub trait VecNumericAggregate {
    type Output;

    fn sum(&self) -> Result<Self::Output> {
        Err(anyhow!("sum unimplemented"))
    }

    fn sum_groups(&self, _groups: &GroupRanges) -> Result<Self::Output> {
        Err(anyhow!("sum groups unimplemented"))
    }

    fn avg(&self) -> Result<Self::Output> {
        Err(anyhow!("avg unimplemented"))
    }

    fn avg_groups(&self, _groups: &GroupRanges) -> Result<Self::Output> {
        Err(anyhow!("avg groups unimplemented"))
    }
}

impl<T: NumericType + FixedLengthType> VecNumericAggregate for FixedLengthVec<T> {
    type Output = Self; // TODO: Change this to numeric.

    fn sum(&self) -> Result<Self::Output> {
        Ok(agg_fixedlen_init_first(self, |v| v, |acc, v| acc + v))
    }

    fn sum_groups(&self, groups: &GroupRanges) -> Result<Self::Output> {
        Ok(agg_fixedlen_init_first_groups(
            self,
            groups,
            |v| v,
            |acc, v| acc + v,
        ))
    }

    fn avg(&self) -> Result<Self::Output> {
        unimplemented!()
    }
}

impl VecNumericAggregate for BoolVec {
    type Output = Self;
}
impl VecNumericAggregate for Utf8Vec {
    type Output = Self;
}

impl VecNumericAggregate for ValueVec {
    type Output = Self;

    fn sum(&self) -> Result<Self::Output> {
        Ok(value_vec_dispatch_unary!(self, VecNumericAggregate::sum))
    }

    fn sum_groups(&self, groups: &GroupRanges) -> Result<Self::Output> {
        Ok(value_vec_dispatch_unary_groups!(
            self,
            groups,
            VecNumericAggregate::sum_groups
        ))
    }

    fn avg(&self) -> Result<Self::Output> {
        Ok(value_vec_dispatch_unary!(self, VecNumericAggregate::avg))
    }
}
