use glaredb_error::{RayexecError, Result};

use crate::arrays::datatype::{DataType, DecimalTypeMeta};
use crate::arrays::scalar::decimal::DecimalType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CastDecimalTypeInfo {
    /// The precision/scale to cast to.
    pub meta: DecimalTypeMeta,
    /// If the desired precision exceeded the max precision for the decimal type
    /// we're checking.
    pub precision_exceeded: bool,
}

/// Tries to find a precision and scale that can be used to execute arith
/// operators between left and right.
///
/// Errors if we can't find one.
pub fn common_add_sub_decimal_type_info<D>(
    left: &DataType,
    right: &DataType,
) -> Result<CastDecimalTypeInfo>
where
    D: DecimalType,
{
    let l_meta = match D::decimal_meta_opt(left) {
        Some(meta) => meta,
        None => DecimalTypeMeta::new_for_datatype_id(left.datatype_id())
            .ok_or_else(|| RayexecError::new(format!("Cannot convert {left} into a decimal")))?,
    };

    let r_meta = match D::decimal_meta_opt(right) {
        Some(meta) => meta,
        None => DecimalTypeMeta::new_for_datatype_id(right.datatype_id())
            .ok_or_else(|| RayexecError::new(format!("Cannot convert {right} into a decimal")))?,
    };

    // Need to apply casts to get the decimals to the same prec/scale.
    let max_scale = i8::max(l_meta.scale, r_meta.scale);

    // TODO: Does this properly handle negative scale?
    let l_int_digits = (l_meta.precision as i8) - l_meta.scale;
    let r_int_digits = (r_meta.precision as i8) - r_meta.scale;

    let max_int_digits = i8::max(l_int_digits, r_int_digits);

    let mut new_prec = (max_int_digits + max_scale) as u8;

    // Try to avoid overflow.
    new_prec += 1;

    let mut precision_exceeded = false;
    if new_prec > D::MAX_PRECISION {
        // Truncate to max precision this decimal type can handle.
        // Casting may fail at runtime.
        new_prec = D::MAX_PRECISION;
        precision_exceeded = true;
    }

    Ok(CastDecimalTypeInfo {
        meta: DecimalTypeMeta {
            precision: new_prec,
            scale: max_scale,
        },
        precision_exceeded,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrays::scalar::decimal::Decimal64Type;

    #[test]
    fn decimals_with_same_precision_scale() {
        let left = DataType::Decimal64(DecimalTypeMeta::new(9, 3));
        let right = DataType::Decimal64(DecimalTypeMeta::new(9, 3));

        let info = common_add_sub_decimal_type_info::<Decimal64Type>(&left, &right).unwrap();

        assert!(!info.precision_exceeded);
        assert_eq!(10, info.meta.precision);
        assert_eq!(3, info.meta.scale);
    }

    #[test]
    fn decimals_with_different_precision_scale() {
        let left = DataType::Decimal64(DecimalTypeMeta::new(9, 3));
        let right = DataType::Decimal64(DecimalTypeMeta::new(4, 1));

        let info = common_add_sub_decimal_type_info::<Decimal64Type>(&left, &right).unwrap();

        assert!(!info.precision_exceeded);
        assert_eq!(10, info.meta.precision);
        assert_eq!(3, info.meta.scale);
    }

    #[test]
    fn decimal_with_i32() {
        let left = DataType::Decimal64(DecimalTypeMeta::new(9, 3));
        let right = DataType::Int32;

        let info = common_add_sub_decimal_type_info::<Decimal64Type>(&left, &right).unwrap();

        assert!(!info.precision_exceeded);
        assert_eq!(14, info.meta.precision);
        assert_eq!(3, info.meta.scale);
    }
}
