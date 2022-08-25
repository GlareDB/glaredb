//! Transparent float wrappers providing total order for floats.
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::ops::{Add, Deref, DerefMut, Div, Mul, Sub};

macro_rules! trait_impls {
    ($wrapper:ident, $prim:ident) => {
        // From/Deref

        impl From<$prim> for $wrapper {
            fn from(v: $prim) -> Self {
                Self(v)
            }
        }

        impl From<$wrapper> for $prim {
            fn from(v: $wrapper) -> Self {
                v.0
            }
        }

        impl Deref for $wrapper {
            type Target = $prim;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl DerefMut for $wrapper {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        // Eq/Ord

        impl PartialEq for $wrapper {
            fn eq(&self, other: &Self) -> bool {
                $prim::total_cmp(&self, other).is_eq()
            }
        }

        impl Eq for $wrapper {}

        impl PartialOrd for $wrapper {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some($prim::total_cmp(self, other))
            }
        }

        impl Ord for $wrapper {
            fn cmp(&self, other: &Self) -> Ordering {
                $prim::total_cmp(&self, other)
            }
        }

        // Arith

        impl Add for $wrapper {
            type Output = $wrapper;
            fn add(self, other: Self) -> $wrapper {
                (self.0 + other.0).into()
            }
        }

        impl Sub for $wrapper {
            type Output = $wrapper;
            fn sub(self, other: Self) -> $wrapper {
                (self.0 - other.0).into()
            }
        }

        impl Div for $wrapper {
            type Output = $wrapper;
            fn div(self, other: Self) -> $wrapper {
                (self.0 / other.0).into()
            }
        }

        impl Mul for $wrapper {
            type Output = $wrapper;
            fn mul(self, other: Self) -> $wrapper {
                (self.0 * other.0).into()
            }
        }
    };
}

/// A totally ordered float32.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[repr(transparent)]
pub struct OrdF32(f32);

trait_impls!(OrdF32, f32);

/// A totally ordered float64.
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize)]
#[repr(transparent)]
pub struct OrdF64(f64);

trait_impls!(OrdF64, f64);
