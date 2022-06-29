/// Provides types for totally ordered floats.
///
/// The `NotNanF..` types provide float wrappers to disallow constructing NaN
/// floats.
///
/// Eventually we do want to support NaNs, and it might be best to follow what
/// Postgres does.
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Deref;

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NotNanF32(f32);

#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct NotNanF64(f64);

macro_rules! impl_not_nan {
    ($not_nan:ident, $prim:ty) => {
        impl $not_nan {
            pub fn new(f: $prim) -> Option<$not_nan> {
                if f.is_nan() {
                    return None;
                }
                Some($not_nan(f))
            }
        }

        impl Deref for $not_nan {
            type Target = $prim;
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl TryFrom<$prim> for $not_nan {
            type Error = ();
            fn try_from(val: $prim) -> Result<Self, ()> {
                match Self::new(val) {
                    Some(n) => Ok(n),
                    None => Err(()),
                }
            }
        }

        impl Into<$prim> for $not_nan {
            fn into(self) -> $prim {
                self.0
            }
        }

        impl AsRef<$prim> for $not_nan {
            fn as_ref(&self) -> &$prim {
                &self.0
            }
        }

        impl Eq for $not_nan {}

        impl Ord for $not_nan {
            fn cmp(&self, other: &Self) -> Ordering {
                // Construction of the not nan types ensure that the value is
                // never NaN, and that would be the way that this would return
                // `None`.
                self.0.partial_cmp(&other.0).unwrap()
            }
        }

        impl fmt::Display for $not_nan {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

impl_not_nan!(NotNanF32, f32);
impl_not_nan!(NotNanF64, f64);
