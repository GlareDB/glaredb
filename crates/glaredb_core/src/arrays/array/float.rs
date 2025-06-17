use half::f16;

use super::physical_type::{PhysicalF16, PhysicalF32, PhysicalF64, ScalarStorage};

/// Extension for floats.
pub trait FloatScalarStorage: ScalarStorage
where
    Self::StorageType: Sized,
{
    /// First 39 powers of ten.
    ///
    /// Should be used as an alternative to `powi`, specifically when converting
    /// decimals to floats since we want exact values.
    const POWERS_OF_10: [Self::StorageType; 39];
}

impl FloatScalarStorage for PhysicalF16 {
    const POWERS_OF_10: [Self::StorageType; 39] = {
        let mut table = [f16::from_f32_const(1.0); 39];
        let mut idx = 0;
        while idx < 39 {
            // Some values will end up being infinity.
            table[idx] = f16::from_f32_const(PhysicalF32::POWERS_OF_10[idx]);
            idx += 1;
        }

        table
    };
}

impl FloatScalarStorage for PhysicalF32 {
    const POWERS_OF_10: [Self::StorageType; 39] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
        1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38,
    ];
}

impl FloatScalarStorage for PhysicalF64 {
    const POWERS_OF_10: [Self::StorageType; 39] = [
        1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
        1e17, 1e18, 1e19, 1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31,
        1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38,
    ];
}
