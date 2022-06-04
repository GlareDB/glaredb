use std::fmt;

/// Utility wrapper for implementing `Display` for a slice.
///
/// ``` rust
/// use fmtutil::DisplaySlice;
///
/// let v = vec![1, 2, 3];
/// let s = format!("{}", DisplaySlice(&v));
/// assert_eq!("[1, 2, 3]", &s);
/// ```
///
/// This type should only be used during formatting user-facing output.
#[derive(Debug)]
pub struct DisplaySlice<'a, T>(pub &'a [T]);

impl<'a, T: fmt::Display> fmt::Display for DisplaySlice<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let iter = self.0.iter();
        let len = iter.len();

        write!(f, "[")?;
        for (i, item) in iter.enumerate() {
            write!(f, "{}", item)?;
            if len - i > 1 {
                write!(f, ", ")?;
            }
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_list() {
        // (input, expected)
        let tests = vec![
            (vec![], "[]"),
            (vec![1], "[1]"),
            (vec![1, 2, 3], "[1, 2, 3]"),
        ];

        for (input, expected) in tests.into_iter() {
            let s = format!("{}", DisplaySlice(&input));
            assert_eq!(expected, &s);
        }
    }
}
