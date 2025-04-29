use std::cmp;

use glaredb_error::{DbError, Result};

use crate::arrays::array::physical_type::PhysicalType;
use crate::arrays::string::StringPtr;

/// Returns if comparing heap values of the given type is supported.
pub const fn compare_heap_value_supported(phys_type: PhysicalType) -> bool {
    // TODO: Structs and lists
    matches!(phys_type, PhysicalType::Binary | PhysicalType::Utf8)
}

/// Compares two values sitting in heap blocks.
///
/// # Safety
///
/// The pointers provided must correspond to the types that we write for the
/// given values.
pub unsafe fn compare_heap_values(
    left: *const u8,
    right: *const u8,
    phys_type: PhysicalType,
) -> Result<cmp::Ordering> {
    match phys_type {
        PhysicalType::Binary | PhysicalType::Utf8 => {
            let left_str = unsafe { left.cast::<StringPtr>().read_unaligned() };
            let right_str = unsafe { right.cast::<StringPtr>().read_unaligned() };
            Ok(left_str.as_bytes().cmp(right_str.as_bytes()))
        }
        other => Err(DbError::new(format!(
            "Unsupported heap value comparison for type: {other}"
        ))),
    }
}
