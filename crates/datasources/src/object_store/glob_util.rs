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

        let start: i64 = start.parse().map_err(|e| {
            ReplacerFindError::new(format!("start in range not a valid number: {e}"))
        })?;
        let end: i64 = end
            .parse()
            .map_err(|e| ReplacerFindError::new(format!("end in range not a valid number: {e}")))?;

        (start..=end).map(|i| i.to_string()).collect::<Vec<_>>()
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
            assert_eq!(&actual, expected);
        }

        // TODO: Add test cases for failed patterns...
    }
}
