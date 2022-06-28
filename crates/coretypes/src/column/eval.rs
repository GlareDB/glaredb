use super::vec::*;

// TODO: More proper handling of nulls and mixed length vectors.

pub trait SqlEq<Rhs = Self> {
    /// Equality comparison with sql semantics.
    fn sql_eq(&self, rhs: &Rhs) -> BoolVec;
}

impl<T: FixedLengthType> SqlEq for FixedLengthVec<T> {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        assert_eq!(self.len(), rhs.len());
        self.eval_binary_produce_fixed(rhs, |a, b| a == b)
    }
}

impl<T: BytesRef + ?Sized> SqlEq for VarLengthVec<T> {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        assert_eq!(self.len(), rhs.len());
        self.eval_binary_produce_fixed(rhs, |a, b| a == b)
    }
}

impl SqlEq for ColumnVec {
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

impl SqlEq for NullableColumnVec {
    fn sql_eq(&self, rhs: &Self) -> BoolVec {
        // TODO: Handle nulls.
        let a = self.get_values();
        let b = rhs.get_values();
        a.sql_eq(b)
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
}
