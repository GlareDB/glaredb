use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::{Result, ResultExt};
use globset::{GlobBuilder, GlobMatcher};

use super::FileSystem;
use super::file_list::FileList;

/// Result of attempting to expand the next batch of paths.
///
/// Needed since we may receive a batch of paths all don't match the glob. We
/// need to differentiate between that case, and being truely exhausted.
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
        // paths than we want.
        //
        // Instead we should be incrementally building up the prefix to use, or
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

    pub fn expand_next<'a>(&'a mut self, expanded: &'a mut Vec<String>) -> ExpandNext<'a, L> {
        ExpandNext {
            glob: self,
            expanded,
        }
    }

    pub fn expand_all<'a>(&'a mut self, expanded: &'a mut Vec<String>) -> ExpandAll<'a, L> {
        ExpandAll {
            glob: self,
            expanded,
            total: 0,
        }
    }

    pub fn poll_expand(
        &mut self,
        cx: &mut Context,
        expanded: &mut Vec<String>,
    ) -> Poll<Result<ExpandResult>> {
        // TODO: Probably need to track when we clear. List could be using it as
        // a scratch buffer... which would be weird.
        self.list_buf.clear();
        let n = match self.list.poll_list(cx, &mut self.list_buf)? {
            Poll::Ready(n) => n,
            Poll::Pending => return Poll::Pending,
        };
        if n == 0 {
            return Poll::Ready(Ok(ExpandResult::Exhausted));
        }

        let out_len = expanded.len();
        // Extend `out` with only paths that pass the glob matcher.
        expanded.extend(self.list_buf.drain(..).filter(|path| {
            let rel = &path[self.prefix_len..];
            self.matcher.is_match(rel)
        }));
        let append_count = expanded.len() - out_len;

        Poll::Ready(Ok(ExpandResult::Expanded(append_count)))
    }
}

#[derive(Debug)]
pub struct ExpandNext<'a, L: FileList> {
    glob: &'a mut GlobList<L>,
    expanded: &'a mut Vec<String>,
}

impl<'a, L> Future for ExpandNext<'a, L>
where
    L: FileList,
{
    type Output = Result<ExpandResult>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.glob.poll_expand(cx, this.expanded)
    }
}

#[derive(Debug)]
pub struct ExpandAll<'a, L: FileList> {
    glob: &'a mut GlobList<L>,
    expanded: &'a mut Vec<String>,
    total: usize,
}

impl<'a, L> Future for ExpandAll<'a, L>
where
    L: FileList,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let expand_res = match this.glob.poll_expand(cx, this.expanded) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            match expand_res {
                ExpandResult::Expanded(n) => {
                    this.total += n; // 'n' may be zero.
                    // Continue... keep expanding.
                }
                ExpandResult::Exhausted => return Poll::Ready(Ok(this.total)),
            }
        }
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
