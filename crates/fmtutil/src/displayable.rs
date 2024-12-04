use std::fmt::{self, Display};

/// Helper trait for easily creating a `DisplayableSlice` from an existing slice
/// or vec.
pub trait IntoDisplayableSlice<T: Display> {
    /// Displays a slice wrapped with '[' and ']' with each element separated by
    /// a comma.
    fn display_with_brackets(&self) -> DisplayableSlice<T>;

    /// Displays a slice as a comma separated list.
    fn display_as_list(&self) -> DisplayableSlice<T>;
}

impl<T: Display> IntoDisplayableSlice<T> for &[T] {
    fn display_with_brackets(&self) -> DisplayableSlice<T> {
        DisplayableSlice {
            left_delim: "[",
            right_delim: "]",
            slice: self,
        }
    }

    fn display_as_list(&self) -> DisplayableSlice<T> {
        DisplayableSlice {
            left_delim: "",
            right_delim: "",
            slice: self,
        }
    }
}

impl<T: Display> IntoDisplayableSlice<T> for [T] {
    fn display_with_brackets(&self) -> DisplayableSlice<T> {
        DisplayableSlice {
            left_delim: "[",
            right_delim: "]",
            slice: self,
        }
    }

    fn display_as_list(&self) -> DisplayableSlice<T> {
        DisplayableSlice {
            left_delim: "",
            right_delim: "",
            slice: self,
        }
    }
}

impl<T: Display> IntoDisplayableSlice<T> for Vec<T> {
    fn display_with_brackets(&self) -> DisplayableSlice<T> {
        DisplayableSlice {
            left_delim: "[",
            right_delim: "]",
            slice: self,
        }
    }

    fn display_as_list(&self) -> DisplayableSlice<T> {
        DisplayableSlice {
            left_delim: "",
            right_delim: "",
            slice: self,
        }
    }
}

/// Wrapper around a slice that implements display formatting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DisplayableSlice<'a, T: Display> {
    pub left_delim: &'static str,
    pub right_delim: &'static str,
    pub slice: &'a [T],
}

impl<'a, T: Display, V: AsRef<[T]>> From<&'a V> for DisplayableSlice<'a, T> {
    fn from(value: &'a V) -> Self {
        DisplayableSlice {
            left_delim: "[",
            right_delim: "]",
            slice: value.as_ref(),
        }
    }
}

impl<T: Display> Display for DisplayableSlice<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.left_delim)?;
        for (idx, item) in self.slice.iter().enumerate() {
            if idx > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{item}")?;
        }
        write!(f, "{}", self.right_delim)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_slice() {
        let slice = ["a", "b", "c"];
        let s = slice.display_with_brackets().to_string();
        assert_eq!("[a, b, c]", s);
    }

    #[test]
    fn display_with_empty_delims() {
        let slice = &["a", "b", "c"];
        let s = DisplayableSlice {
            left_delim: "",
            right_delim: "",
            slice,
        };
        assert_eq!("a, b, c", s.to_string());
    }
}
