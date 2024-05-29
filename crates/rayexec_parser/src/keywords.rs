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
    ($($ident:ident),* $(,)?) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

// Keep keywords sorted to allow for binary search.
#[rustfmt::skip]
define_keywords!(
    ALL,
    ANALYZE,
    AND,
    ANTI,
    ANY,
    AS,
    ASC,
    BEGIN,
    BETWEEN,
    BIGDECIMAL,
    BIGINT,
    BIGNUMERIC,
    BINARY,
    BOOL,
    BOOLEAN,
    BY,
    CASE,
    CLUSTER,
    CREATE,
    CROSS,
    CUBE,
    DATABASE,
    DESC,
    DISTINCT,
    DISTRIBUTE,
    DOUBLE,
    END,
    EXCEPT,
    EXISTS,
    EXPLAIN,
    EXTERNAL,
    FALSE,
    FETCH,
    FILTER,
    FIRST,
    FLOAT,
    FLOAT4,
    FLOAT8,
    FORMAT,
    FROM,
    FULL,
    GROUP,
    GROUPING,
    HAVING,
    IF,
    ILIKE,
    IN,
    INNER,
    INSERT,
    INT,
    INT2,
    INT4,
    INT8,
    INTEGER,
    INTERSECT,
    INTO,
    IS,
    JOIN,
    JSON,
    LAST,
    LATERAL,
    LEFT,
    LIKE,
    LIMIT,
    NATURAL,
    NOT,
    NULL,
    NULLS,
    OFFSET,
    ON,
    OR,
    ORDER,
    OUTER,
    PARTITION,
    PIVOT,
    PRIMARY,
    QUALIFY,
    REAL,
    RECURSIVE,
    REGEXP,
    REPLACE,
    RIGHT,
    RLIKE,
    ROLLBACK,
    ROLLUP,
    SCHEMA,
    SELECT,
    SEMI,
    SET,
    SETS,
    SHOW,
    SIMILAR,
    SMALLINT,
    SORT,
    STRING,
    TABLE,
    TEMP,
    TEMPORARY,
    TEXT,
    TO,
    TOP,
    TRUE,
    UNION,
    UNPIVOT,
    USING,
    VALUES,
    VARCHAR,
    VERBOSE,
    VIEW,
    WHERE,
    WINDOW,
    WITH,
);

/// These keywords can't be used as a table alias, so that `FROM table_name alias`
/// can be parsed unambiguously without looking ahead.
pub const RESERVED_FOR_TABLE_ALIAS: &[Keyword] = &[
    // Reserved as both a table and a column alias:
    Keyword::WITH,
    Keyword::EXPLAIN,
    Keyword::ANALYZE,
    Keyword::SELECT,
    Keyword::WHERE,
    Keyword::GROUP,
    Keyword::SORT,
    Keyword::HAVING,
    Keyword::ORDER,
    Keyword::PIVOT,
    Keyword::UNPIVOT,
    Keyword::TOP,
    Keyword::LATERAL,
    Keyword::VIEW,
    Keyword::LIMIT,
    Keyword::OFFSET,
    Keyword::FETCH,
    Keyword::UNION,
    Keyword::EXCEPT,
    Keyword::INTERSECT,
    // Reserved only as a table alias in the `FROM`/`JOIN` clauses:
    Keyword::ON,
    Keyword::JOIN,
    Keyword::INNER,
    Keyword::CROSS,
    Keyword::FULL,
    Keyword::LEFT,
    Keyword::RIGHT,
    Keyword::NATURAL,
    Keyword::USING,
    Keyword::CLUSTER,
    Keyword::DISTRIBUTE,
    // for MSSQL-specific OUTER APPLY (seems reserved in most dialects)
    Keyword::OUTER,
    Keyword::SET,
    Keyword::QUALIFY,
    Keyword::WINDOW,
    Keyword::END,
    // for MYSQL PARTITION SELECTION
    Keyword::PARTITION,
];

/// Can't be used as a column alias, so that `SELECT <expr> alias`
/// can be parsed unambiguously without looking ahead.
pub const RESERVED_FOR_COLUMN_ALIAS: &[Keyword] = &[
    // Reserved as both a table and a column alias:
    Keyword::WITH,
    Keyword::EXPLAIN,
    Keyword::ANALYZE,
    Keyword::SELECT,
    Keyword::WHERE,
    Keyword::GROUP,
    Keyword::SORT,
    Keyword::HAVING,
    Keyword::ORDER,
    Keyword::TOP,
    Keyword::LATERAL,
    Keyword::VIEW,
    Keyword::LIMIT,
    Keyword::OFFSET,
    Keyword::FETCH,
    Keyword::UNION,
    Keyword::EXCEPT,
    Keyword::INTERSECT,
    Keyword::CLUSTER,
    Keyword::DISTRIBUTE,
    // Reserved only as a column alias in the `SELECT` clause
    Keyword::FROM,
    Keyword::INTO,
    Keyword::END,
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    #[test]
    fn keywords_sorted() {
        let mut prev = KEYWORD_STRINGS[0];
        for curr in &KEYWORD_STRINGS[1..] {
            assert_eq!(prev.cmp(curr), Ordering::Less, "prev: {prev}, curr: {curr}");
            prev = *curr;
        }
    }

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
