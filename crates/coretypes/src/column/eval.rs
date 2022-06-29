use super::vec::*;
use crate::datatype::DataValue;

// TODO: More proper handling of nulls and mixed length vectors.

pub trait SqlCmp<Rhs: ?Sized = Self> {
    /// Equality comparison with sql semantics.
    fn sql_eq(&self, rhs: &Rhs) -> BoolVec;
}

impl<T: FixedLengthType> SqlCmp for FixedLengthVec<T> {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        assert_eq!(self.len(), rhs.len());
        self.eval_binary_vec(rhs, |a, b| a == b)
    }
}

impl<T: FixedLengthType> SqlCmp<T> for FixedLengthVec<T> {
    fn sql_eq(&self, rhs: &T) -> BoolVec {
        self.eval_unary(|val| val == rhs)
    }
}

impl<T: BytesRef + ?Sized> SqlCmp for VarLengthVec<T> {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        assert_eq!(self.len(), rhs.len());
        self.eval_binary_vec(rhs, |a, b| a == b)
    }
}

impl<T: BytesRef + ?Sized> SqlCmp<T> for VarLengthVec<T> {
    fn sql_eq(&self, rhs: &T) -> BoolVec {
        self.eval_unary_fixed(|val| val == rhs)
    }
}

impl SqlCmp for ColumnVec {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        // TODO: Casting.
        match (self, rhs) {
            (Self::Bool(a), Self::Bool(b)) => a.sql_eq(b),
            (Self::I8(a), Self::I8(b)) => a.sql_eq(b),
            (Self::I16(a), Self::I16(b)) => a.sql_eq(b),
            (Self::I32(a), Self::I32(b)) => a.sql_eq(b),
            (Self::I64(a), Self::I64(b)) => a.sql_eq(b),
            (Self::F32(a), Self::F32(b)) => a.sql_eq(b),
            (Self::F64(a), Self::F64(b)) => a.sql_eq(b),
            (Self::Str(a), Self::Str(b)) => a.sql_eq(b),
            (Self::Binary(a), Self::Binary(b)) => a.sql_eq(b),
            _ => vec![false; self.len()].into(),
        }
    }
}

impl SqlCmp<DataValue> for ColumnVec {
    fn sql_eq(&self, rhs: &DataValue) -> BoolVec {
        // TODO: Casting and missed types.
        match (self, rhs) {
            (Self::Bool(a), DataValue::Bool(b)) => a.sql_eq(b),
            (Self::I8(a), DataValue::Int8(b)) => a.sql_eq(b),
            (Self::I16(a), DataValue::Int16(b)) => a.sql_eq(b),
            (Self::I32(a), DataValue::Int32(b)) => a.sql_eq(b),
            (Self::I64(a), DataValue::Int64(b)) => a.sql_eq(b),
            (Self::F32(a), DataValue::Float32(b)) => a.sql_eq(b.as_ref()),
            (Self::F64(a), DataValue::Float64(b)) => a.sql_eq(b.as_ref()),
            (Self::Str(a), DataValue::Utf8(b)) => {
                let b_ref: &str = b.as_ref();
                a.sql_eq(b_ref)
            }
            (Self::Binary(a), DataValue::Binary(b)) => {
                let b_ref: &[u8] = b.as_ref();
                a.sql_eq(b_ref)
            }
            _ => vec![false; self.len()].into(),
        }
    }
}

impl SqlCmp for NullableColumnVec {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        // TODO: Handle nulls.
        let a = self.get_values();
        let b = rhs.get_values();
        a.sql_eq(b)
    }
}

impl SqlCmp<DataValue> for NullableColumnVec {
    fn sql_eq(&self, rhs: &DataValue) -> BoolVec {
        self.get_values().sql_eq(rhs)
    }
}

impl SqlCmp for DataValue {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        // TODO: Proper null handling.
        let out = self == rhs;
        FixedLengthVec::one(out)
    }
}

pub trait SqlLogic<Rhs: ?Sized = Self> {
    fn sql_and(&self, other: &Rhs) -> BoolVec;
}

impl SqlLogic for FixedLengthVec<bool> {
    fn sql_and(&self, other: &Self) -> BoolVec {
        self.eval_binary_vec(other, |a, b| *a && *b)
    }
}

impl SqlLogic<bool> for FixedLengthVec<bool> {
    fn sql_and(&self, other: &bool) -> BoolVec {
        self.eval_unary(|val| *val && *other)
    }
}

impl SqlLogic for bool {
    fn sql_and(&self, other: &Self) -> BoolVec {
        FixedLengthVec::one(*self && *other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_fixed() {
        let a: FixedLengthVec<i32> = vec![0, 1, 2, 3, 4, 5].into();
        let b: FixedLengthVec<i32> = vec![0, 1, 4, 5, 4, 5].into();
        let expected = vec![true, true, false, false, true, true];

        let out = a.sql_eq(&b);
        let out: Vec<_> = out.iter().cloned().collect();
        assert_eq!(expected, out);
    }

    #[test]
    fn eq_varlen() {
        let a: VarLengthVec<str> = vec!["hello", "world", "goodbye", "world"].into();
        let b: VarLengthVec<str> = vec!["goodbye", "world", "hello", "world"].into();
        let expected = vec![false, true, false, true];

        let out = a.sql_eq(&b);
        let out: Vec<_> = out.iter().cloned().collect();
        assert_eq!(expected, out);
    }

    #[test]
    fn eq_varlen_value() {
        let a: VarLengthVec<str> = vec!["hello", "world", "goodby", "world"].into();
        let b = "world".to_string();
        let expected = vec![false, true, false, true];

        let out = a.sql_eq(b.as_str());
        let out: Vec<_> = out.iter().cloned().collect();
        assert_eq!(expected, out);
    }

    #[test]
    fn and_bools() {
        let a: BoolVec = vec![true, true, false, true].into();
        let b: BoolVec = vec![false, true, false, true].into();
        let expected = vec![false, true, false, true];

        let out: Vec<_> = a.sql_and(&b).iter().cloned().collect();
        assert_eq!(expected, out);
    }
}
