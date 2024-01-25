use regex::Regex;

use super::Lazy;

/// Regex for matching strings delineated by commas. Will match full quoted
/// strings as well.
///
/// Taken from <https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes>
const SPLIT_ON_UNQUOTED_COMMAS: &str = r#""[^"]*"|[^,]+"#;

static COMMA_RE: Lazy<Regex> = Lazy::new(|| Regex::new(SPLIT_ON_UNQUOTED_COMMAS).unwrap());

/// Split a string on commas, preserving quoted strings.
pub(super) fn split_comma_delimited(text: &str) -> Vec<String> {
    COMMA_RE
        .find_iter(text)
        .map(|m| m.as_str().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use datafusion::variable::VarType;

    use super::*;
    use crate::vars::inner::SessionVar;

    #[test]
    fn split_on_commas() {
        struct Test {
            input: &'static str,
            expected: Vec<String>,
        }

        let tests = vec![
            Test {
                input: "",
                expected: Vec::new(),
            },
            Test {
                input: "my_schema",
                expected: vec!["my_schema".to_string()],
            },
            Test {
                input: "a,b,c",
                expected: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            },
            Test {
                input: "a,\"b,c\"",
                expected: vec!["a".to_string(), "\"b,c\"".to_string()],
            },
        ];

        for test in tests {
            let out = split_comma_delimited(test.input);
            assert_eq!(test.expected, out);
        }
    }

    #[test]
    fn user_configurable() {
        const SETTABLE: ServerVar<str> = ServerVar {
            name: "unsettable",
            description: "test",
            value: "test",
            group: "test",
            user_configurable: true,
        };
        let mut var = SessionVar::new(&SETTABLE);
        var.set_from_str("user", VarType::UserDefined).unwrap();
        assert_eq!("user", var.value());

        // Should also be able to be set by the system.
        var.set_from_str("system", VarType::System).unwrap();
        assert_eq!("system", var.value());
    }

    #[test]
    fn not_user_configurable() {
        const UNSETTABLE: ServerVar<str> = ServerVar {
            name: "unsettable",
            description: "test",
            value: "test",
            group: "test",
            user_configurable: false,
        };
        let mut var = SessionVar::new(&UNSETTABLE);
        var.set_from_str("custom", VarType::UserDefined)
            .expect_err("Unsettable var should not be allowed to be set by user");

        assert_eq!("test", var.value());

        // System should be able to set the var.
        var.set_from_str("custom", VarType::System).unwrap();
        assert_eq!("custom", var.value());
    }
}
