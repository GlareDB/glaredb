/// Wrapper around a string that provides optional case-insensitive comparisons.
///
/// This just delegates to `eq_ignore_ascii_case`. No fancy unicode case folding
/// happens.
#[derive(Debug, Clone)]
pub struct AsciiCase<S>
where
    S: AsRef<str> + ?Sized,
{
    s: S,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseCompare {
    CaseSensitive,
    CaseInsensitive,
}

impl<S> AsciiCase<S>
where
    S: AsRef<str>,
{
    pub const fn new(s: S) -> Self {
        AsciiCase { s }
    }

    pub fn eq(&self, other: &str, c: CaseCompare) -> bool {
        match c {
            CaseCompare::CaseSensitive => self.as_str() == other,
            CaseCompare::CaseInsensitive => self.as_str().eq_ignore_ascii_case(other),
        }
    }

    pub fn eq_case_sensitive(&self, other: &str) -> bool {
        self.eq(other, CaseCompare::CaseSensitive)
    }

    pub fn eq_case_insensitive(&self, other: &str) -> bool {
        self.eq(other, CaseCompare::CaseInsensitive)
    }

    /// Returns the underlying string with its original casing.
    pub fn as_str(&self) -> &str {
        self.s.as_ref()
    }
}

impl From<String> for AsciiCase<String> {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

// TODO: Allocating a new string could be surprising.
impl From<&str> for AsciiCase<String> {
    fn from(value: &str) -> Self {
        Self::new(value.to_string())
    }
}

impl<S> AsRef<str> for AsciiCase<S>
where
    S: AsRef<str>,
{
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn eq_case_insensitive() {
        // Pass
        assert!(AsciiCase::new("cat").eq_case_insensitive("cat"));
        assert!(AsciiCase::new("cat").eq_case_insensitive("Cat"));
        assert!(AsciiCase::new("cat").eq_case_insensitive("CAT"));

        assert!(AsciiCase::new("Cat").eq_case_insensitive("cat"));
        assert!(AsciiCase::new("CAT").eq_case_insensitive("cat"));

        // Fail
        assert!(!AsciiCase::new("cat").eq_case_insensitive("dog"));
        assert!(!AsciiCase::new("cat").eq_case_insensitive("Dog"));
        assert!(!AsciiCase::new("cat").eq_case_insensitive("DOG"));
    }

    #[test]
    fn eq_case_sensitive() {
        // Pass
        assert!(AsciiCase::new("cat").eq_case_sensitive("cat"));
        assert!(AsciiCase::new("Cat").eq_case_sensitive("Cat"));
        assert!(AsciiCase::new("CAT").eq_case_sensitive("CAT"));

        // Fail
        assert!(!AsciiCase::new("cat").eq_case_sensitive("Cat"));
        assert!(!AsciiCase::new("cat").eq_case_sensitive("CAT"));
        assert!(!AsciiCase::new("CAT").eq_case_sensitive("Cat"));
    }
}
