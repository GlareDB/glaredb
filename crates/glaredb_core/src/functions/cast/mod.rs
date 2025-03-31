pub mod array;
pub mod behavior;
pub mod format;
pub mod parse;

use std::fmt::Debug;

use glaredb_error::Result;

use crate::arrays::array::Array;
use crate::arrays::datatype::DataType;

pub trait CastFunction: Copy + Debug + Sync + Send + Sized + 'static {
    type State: Sync + Send;

    /// Binds this cast function, returning any state needed to execute the
    /// cast.
    fn bind(src: &DataType, target: &DataType) -> Result<Self::State>;

    /// Cast `src` to the target type, writing the results to `out`.
    fn cast(src: &Array, out: &mut Array) -> Result<()>;
}
