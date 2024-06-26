use std::fmt::{self, Display};

/// Helper trait for easily creating a `DisplayableSlice` from an existing slice
/// or vec.
pub trait IntoDisplayableSlice<T: Display> {
    fn displayable(&self) -> DisplayableSlice<T>;
}

impl<'a, T: Display> IntoDisplayableSlice<T> for &'a [T] {
    fn displayable(&self) -> DisplayableSlice<T> {
        DisplayableSlice { slice: self }
    }
}

impl<T: Display> IntoDisplayableSlice<T> for [T] {
    fn displayable(&self) -> DisplayableSlice<T> {
        DisplayableSlice { slice: self }
    }
}

impl<T: Display> IntoDisplayableSlice<T> for Vec<T> {
    fn displayable(&self) -> DisplayableSlice<T> {
        DisplayableSlice { slice: self }
    }
}

/// Wrapper around a slice that implements display formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayableSlice<'a, T: Display> {
    pub slice: &'a [T],
}

impl<'a, T: Display, V: AsRef<[T]>> From<&'a V> for DisplayableSlice<'a, T> {
    fn from(value: &'a V) -> Self {
        DisplayableSlice {
            slice: value.as_ref(),
        }
    }
}

impl<'a, T: Display> Display for DisplayableSlice<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (idx, item) in self.slice.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_slice() {
        let slice = ["a", "b", "c"];
        let s = slice.displayable().to_string();
        assert_eq!("[a, b, c]", s);
    }
}
