use std::vec::IntoIter;

use once_cell::sync::Lazy;
use regex::Regex;
use tracing::debug;

static VALID_STRING_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new("^[a-zA-Z0-9_-]+$").unwrap());

#[derive(Debug, Clone)]
struct Replacer {
    start: usize,
    end: usize,
    iter: IntoIter<String>,
}

#[derive(Debug, thiserror::Error)]
#[error("Replacer find error: {0}")]
struct ReplacerFindError(String);

impl ReplacerFindError {
    fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

fn get_range_iter(start: &str, end: &str) -> Result<Vec<String>, ReplacerFindError> {
    // Valid cases:
    // N..M -> N and M are both integers
    // N..M -> N and M are both lowercase chars
    // N..M -> N and M are both uppercase chars
    // ..M -> M is a positive integer
    // ..M -> M is a lowercase char
    // ..M -> M is an uppercase char
    // N.. -> N is a lowercase char
    // N.. -> N is an uppercase char

    fn is_char(s: &str) -> Option<char> {
        let mut chars = s.chars();
        if let Some(c) = chars.next() {
            if chars.next().is_none() && c.is_ascii_alphabetic() {
                Some(c)
            } else {
                None
            }
        } else {
            None
        }
    }

    fn parse_int(s: &str) -> Result<i64, ReplacerFindError> {
        s.parse::<i64>()
            .map_err(|e| ReplacerFindError::new(format!("{s} not a valid i64: {e}")))
    }

    match (start, end) {
        (s, e) if s.is_empty() && e.is_empty() => Err(ReplacerFindError::new(
            "both start and end in range are empty strings",
        )),
        (s, e) if s.is_empty() => {
            if let Some(e_char) = is_char(e) {
                let s_char = if e_char.is_ascii_uppercase() {
                    'A'
                } else if e_char.is_ascii_lowercase() {
                    'a'
                } else {
                    unreachable!("e_char should always be an ascii alphabet")
                };
                let v: Vec<_> = (s_char..=e_char).map(|c| c.to_string()).collect();
                Ok(v)
            } else {
                let i = parse_int(e)?;
                if i < 0 {
                    Err(ReplacerFindError::new(format!(
                        "end {i} not a valid positive integer in ..M pattern"
                    )))
                } else {
                    let v: Vec<_> = (0..=i).map(|i| i.to_string()).collect();
                    Ok(v)
                }
            }
        }
        (s, e) if e.is_empty() => {
            let s_char = is_char(s).ok_or_else(|| {
                ReplacerFindError::new(format!("{s} not a valid char in N.. pattern"))
            })?;
            let e_char = if s_char.is_ascii_uppercase() {
                'Z'
            } else if s_char.is_ascii_lowercase() {
                'z'
            } else {
                unreachable!("s_char should always be an ascii alphabet")
            };
            let v: Vec<_> = (s_char..=e_char).map(|c| c.to_string()).collect();
            Ok(v)
        }
        (s, e) => {
            match (is_char(s), is_char(e)) {
                (Some(s_char), Some(e_char)) => {
                    let (s_char, e_char) = if (s_char.is_ascii_uppercase()
                        && e_char.is_ascii_uppercase())
                        || (s_char.is_ascii_lowercase() && e_char.is_ascii_lowercase())
                    {
                        if s_char < e_char {
                            (s_char, e_char)
                        } else {
                            (e_char, s_char)
                        }
                    } else {
                        return Err(ReplacerFindError::new(format!(
                            "{s} and {e} are invalid chars for range in N..M pattern"
                        )));
                    };
                    let v: Vec<_> = (s_char..=e_char).map(|c| c.to_string()).collect();
                    Ok(v)
                }
                (Some(_), None) | (None, Some(_)) => Err(ReplacerFindError::new(format!(
                    "{s} and {e} are not valid for range in N..M pattern"
                ))),
                (None, None) => {
                    // Both might be integers.
                    let s_int = parse_int(s)?;
                    let e_int = parse_int(e)?;
                    let (s_int, e_int) = if s_int < e_int {
                        (s_int, e_int)
                    } else {
                        (e_int, s_int)
                    };
                    let v: Vec<_> = (s_int..=e_int).map(|i| i.to_string()).collect();
                    Ok(v)
                }
            }
        }
    }
}

fn get_replacer(pattern: &str) -> Result<Replacer, ReplacerFindError> {
    fn validate_string(s: &str) -> Result<String, ReplacerFindError> {
        if VALID_STRING_REGEX.is_match(s) {
            Ok(s.to_string())
        } else {
            Err(ReplacerFindError::new(format!(
                "invalid string in comma seperated values: {s}"
            )))
        }
    }

    let start = pattern
        .find('{')
        .ok_or_else(|| ReplacerFindError::new("no { found"))?;
    if start == pattern.len() - 1 {
        return Err(ReplacerFindError::new("{ found at the end of pattern"));
    }

    let end = pattern[start..pattern.len()]
        .find('}')
        .ok_or_else(|| ReplacerFindError::new("no matching } found"))?;
    let end = start + end; // Since we sliced to find the end

    let pattern = &pattern[start + 1..end];

    let ranges = if pattern.contains(',') {
        pattern
            .split(',')
            .map(validate_string)
            .collect::<Result<Vec<_>, _>>()?
    } else if pattern.contains("..") {
        let (start, end) = pattern
            .split_once("..")
            .ok_or_else(|| ReplacerFindError::new(format!("expected range N..M: {pattern}")))?;

        get_range_iter(start, end)?
    } else {
        vec![validate_string(pattern)?]
    };

    Ok(Replacer {
        start,
        end,
        iter: ranges.into_iter(),
    })
}

pub fn get_resolved_patterns(pattern: String) -> Vec<ResolvedPattern> {
    let Replacer { start, end, iter } = match get_replacer(&pattern) {
        Ok(replacer) => replacer,
        Err(error) => {
            debug!(%error, %pattern, "cannot resolve pattern");
            return vec![ResolvedPattern(pattern.to_owned())];
        }
    };

    iter.flat_map(|replacement| {
        let begin = &pattern[0..start];
        let end = &pattern[end + 1..pattern.len()];
        let new_pattern = format!("{begin}{replacement}{end}");
        get_resolved_patterns(new_pattern)
    })
    .collect()
}

#[derive(Debug, Clone)]
pub struct ResolvedPattern(String);

impl From<ResolvedPattern> for String {
    fn from(ResolvedPattern(string): ResolvedPattern) -> Self {
        string
    }
}

impl AsRef<str> for ResolvedPattern {
    fn as_ref(&self) -> &str {
        let ResolvedPattern(string) = self;
        string
    }
}

#[cfg(test)]
mod tests {
    use super::get_resolved_patterns;

    #[test]
    fn test_resolve_glob_patterns() {
        let test_cases: &[(&str, &[&str])] = &[
            ("file_{3..6}", &["file_3", "file_4", "file_5", "file_6"]),
            ("file_{6..3}", &["file_3", "file_4", "file_5", "file_6"]),
            ("file_{p..r}", &["file_p", "file_q", "file_r"]),
            ("file_{r..p}", &["file_p", "file_q", "file_r"]),
            ("file_{..2}", &["file_0", "file_1", "file_2"]),
            ("file_{..C}", &["file_A", "file_B", "file_C"]),
            ("file_{..c}", &["file_a", "file_b", "file_c"]),
            ("file_{X..}", &["file_X", "file_Y", "file_Z"]),
            ("file_{x..}", &["file_x", "file_y", "file_z"]),
            ("file_{x,y,z}", &["file_x", "file_y", "file_z"]),
            (
                "file_0{1..3}.csv",
                &["file_01.csv", "file_02.csv", "file_03.csv"],
            ),
            (
                "file_{a-b,c-d,e-f}.json",
                &["file_a-b.json", "file_c-d.json", "file_e-f.json"],
            ),
            (
                "file_{0..2}_{p,q,r}.parquet",
                &[
                    "file_0_p.parquet",
                    "file_0_q.parquet",
                    "file_0_r.parquet",
                    "file_1_p.parquet",
                    "file_1_q.parquet",
                    "file_1_r.parquet",
                    "file_2_p.parquet",
                    "file_2_q.parquet",
                    "file_2_r.parquet",
                ],
            ),
        ];

        for (pattern, expected) in test_cases {
            let patterns = get_resolved_patterns(pattern.to_string());
            let actual = patterns.iter().map(AsRef::as_ref).collect::<Vec<_>>();
            assert_eq!(&actual, expected, "pattern = {pattern}");
        }

        // TODO: Add test cases for failed patterns...
    }
}
