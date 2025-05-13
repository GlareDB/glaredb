use glaredb_error::{Result, ResultExt};
use globset::{GlobBuilder, GlobMatcher};

use super::FileSystem;
use super::file_list::{FileList, FileListExt};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpandResult {
    Expanded(usize),
    Exhausted,
}

#[derive(Debug)]
pub struct GlobList<L: FileList> {
    list: L,
    matcher: GlobMatcher,
    list_buf: Vec<String>,
    prefix_len: usize,
}

impl<L> GlobList<L>
where
    L: FileList,
{
    pub fn try_new<F>(glob: &str, fs: &F, state: &F::State) -> Result<Self>
    where
        F: FileSystem<FileList = L> + ?Sized,
    {
        // TODO: This will only do a single list which will likely iterate more
        // paths that we want.
        //
        // Instead we should be incrementing building up the prefix to use, or
        // use some sort of "prefix stack" to keep listing until we're
        // exhausted.

        let (prefix, pattern) = split_prefix_glob(glob);
        let list = fs.list_prefix(prefix, state);

        let matcher = GlobBuilder::new(pattern)
            .literal_separator(true) // Do not allow '*' or '?' to match path separators.
            .build()
            .context("Failed to build glob matcher")?
            .compile_matcher();

        Ok(GlobList {
            list,
            matcher,
            list_buf: Vec::new(),
            prefix_len: prefix.len(),
        })
    }

    pub async fn expand_next(&mut self, out: &mut Vec<String>) -> Result<ExpandResult> {
        self.list_buf.clear();
        let n = self.list.list(&mut self.list_buf).await?;
        if n == 0 {
            return Ok(ExpandResult::Exhausted);
        }

        let out_len = out.len();
        // Extend `out` with only paths that pass the glob matcher.
        out.extend(self.list_buf.drain(..).filter(|path| {
            let rel = &path[self.prefix_len..];
            self.matcher.is_match(rel)
        }));
        let append_count = out.len() - out_len;

        Ok(ExpandResult::Expanded(append_count))
    }
}

/// Returns (prefix, glob) where prefix is a static prefix.
fn split_prefix_glob(pattern: &str) -> (&str, &str) {
    let glob_pos = pattern
        .find(|c| c == '*' || c == '?' || c == '[' || c == '{')
        .unwrap_or(pattern.len());
    // Find last path separator before that glob char.
    let slash_pos = pattern[..glob_pos]
        .rfind(|c| c == '/' || c == '\\')
        .map(|i| i + 1)
        .unwrap_or(0);

    (&pattern[..slash_pos], &pattern[slash_pos..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_prefix_tests() {
        struct TestCase {
            input: &'static str,
            prefix: &'static str,
            pattern: &'static str,
        }

        let cases = [
            TestCase {
                input: "*.parquet",
                prefix: "",
                pattern: "*.parquet",
            },
            TestCase {
                input: "./*.parquet",
                prefix: "./",
                pattern: "*.parquet",
            },
            TestCase {
                input: "s3://bucket/*.parquet",
                prefix: "s3://bucket/",
                pattern: "*.parquet",
            },
            TestCase {
                input: "s3://bucket/**/file.parquet",
                prefix: "s3://bucket/",
                pattern: "**/file.parquet",
            },
        ];

        for case in cases {
            let (prefix, pattern) = split_prefix_glob(&case.input);
            assert_eq!(case.prefix, prefix);
            assert_eq!(case.pattern, pattern);
        }
    }
}
