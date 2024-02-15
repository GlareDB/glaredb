use std::hash::Hash;

/// Try to get a keyword from a string, ignoring string casing.
pub fn keyword_from_str(s: &str) -> Option<Keyword> {
    let s = unicase::Ascii::new(s);
    let idx = match KEYWORD_STRINGS.binary_search(&s) {
        Ok(idx) => idx,
        Err(_) => return None,
    };
    Some(ALL_KEYWORDS[idx])
}

/// Generate an enum of keywords.
macro_rules! define_keywords {
    ($($ident:ident),*) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum Keyword {
            $($ident),*
        }

        pub const ALL_KEYWORDS: &'static [Keyword] = &[
            $(Keyword::$ident),*
        ];

        pub const KEYWORD_STRINGS: &'static [unicase::Ascii<&'static str>] = &[
            $(unicase::Ascii::new(stringify!($ident)),)*
        ];
    };
}

#[rustfmt::skip]
define_keywords!(
    BEGIN,
    BETWEEN,
    BIGDECIMAL,
    BIGINT,
    BIGNUMERIC,
    BINARY,
    BY,
    CASE,
    CREATE,
    DATABASE,
    EXISTS,
    EXPLAIN,
    FALSE,
    FROM,
    IF,
    INSERT,
    INT,
    INT2,
    INT4,
    INT8,
    INTEGER,
    INTO,
    NOT,
    OR,
    ORDER,
    PRIMARY,
    REPLACE,
    ROLLBACK,
    SCHEMA,
    SELECT,
    SET,
    SHOW,
    TABLE,
    TEMP,
    TEMPORARY,
    TRUE,
    UNION,
    USING,
    VALUES,
    VERBOSE,
    VIEW,
    WHERE,
    WITH
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn case_insensitive() {
        // (input, expected)
        let tests = [
            ("select", Some(Keyword::SELECT)),
            ("SeLeCt", Some(Keyword::SELECT)),
            ("SELECT", Some(Keyword::SELECT)),
            ("NOSELECT", None),
            ("order", Some(Keyword::ORDER)),
        ];

        for (input, expected) in tests {
            let got = keyword_from_str(input);
            assert_eq!(expected, got);
        }
    }
}
