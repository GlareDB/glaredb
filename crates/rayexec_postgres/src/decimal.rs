use rayexec_error::RayexecError;
use rayexec_execution::arrays::scalar::decimal::Decimal128Scalar;
use tokio_postgres::types::{FromSql, Type};

const NBASE: i128 = 10000;
const SIGN_NEG: i16 = 0x4000;

#[derive(Debug)]
pub struct PostgresDecimal(pub Decimal128Scalar);

impl<'a> FromSql<'a> for PostgresDecimal {
    fn from_sql(
        _ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if raw.len() < 8 {
            return Err(Box::new(RayexecError::new(
                "binary buffer too small for decimal",
            )));
        }

        // <https://github.com/postgres/postgres/blob/b2be5cb2ab671073ec0fc69357c3c11e25bb41cc/src/backend/utils/adt/numeric.c#L310>
        let ndigits = i16::from_be_bytes([raw[0], raw[1]]);
        let weight = i16::from_be_bytes([raw[2], raw[3]]);
        let sign = i16::from_be_bytes([raw[4], raw[5]]);
        let dscale = i16::from_be_bytes([raw[6], raw[7]]);

        if raw.len() < 8 + ndigits as usize * 2 {
            return Err(Box::new(RayexecError::new(
                "binary buffer missing digits buffer",
            )));
        }

        let mut result: i128 = 0;

        let digit_buf = &raw[8..8 + ndigits as usize * 2];

        for chunk in digit_buf.chunks(2) {
            let digit = u16::from_be_bytes([chunk[0], chunk[1]]) as i128;
            result = result * NBASE + digit;
        }

        // Apply the scale by dividing the result by 10^dscale
        let scale_adjustment = 10_i128.pow(dscale as u32);
        result = if weight >= 0 {
            result * 10_i128.pow((weight as u32) * 4) / scale_adjustment
        } else {
            result / (scale_adjustment * 10_i128.pow((-weight as u32) * 4))
        };

        if sign == SIGN_NEG {
            result = -result;
        }

        let precision = (ndigits as u8) * 4; // Each base-10000 digit represents up to 4 decimal digits

        Ok(PostgresDecimal(Decimal128Scalar {
            precision,
            scale: dscale as i8,
            value: result,
        }))
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::NUMERIC)
    }
}
