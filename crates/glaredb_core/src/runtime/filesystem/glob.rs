use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result, ResultExt};
use globset::{GlobBuilder, GlobMatcher};

use super::FileSystem;
use super::dir_list::{DirEntry, DirList};
use super::file_provider::FileProvider;

#[derive(Debug)]
pub struct GlobList<L: DirList> {
    matcher: GlobMatcher,
    list_buf: Vec<DirEntry>,
    /// (dir, dir_list)
    stack: Vec<(String, L)>,
}

impl<L> GlobList<L>
where
    L: DirList,
{
    pub fn try_new<F>(fs: &F, state: &F::State, glob: &str) -> Result<Self>
    where
        F: FileSystem<DirList = L> + ?Sized,
    {
        // TODO: This will only do a single list which will likely iterate more
        // paths than we want.
        //
        // Instead we should be incrementally building up the prefix to use, or
        // use some sort of "prefix stack" to keep listing until we're
        // exhausted.

        let mut segments = split_segments(glob);
        if segments.is_empty() {
            return Err(DbError::new("Missing segments for glob"));
        }

        // Find the root dir to use.
        let mut root = Vec::new();
        while !segments.is_empty() && !is_glob(&segments[0]) {
            root.push(segments.remove(0));
        }

        // TODO: Probably needs to be os/filesystem specific.
        //
        // E.g. for s3, always use '/'. If local and windows, use '\'.
        let root = root.join("/");

        // Build matcher for original glob.
        let matcher = GlobBuilder::new(glob)
            .literal_separator(true)
            .build()
            .context("Failed to build glob matcher")?
            .compile_matcher();

        unimplemented!()
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
    ) -> Poll<Result<usize>> {
        loop {
            unimplemented!()
            // // TODO: Probably need to track when we clear. List could be using it as
            // // a scratch buffer... which would be weird.
            // self.list_buf.clear();
            // let n = match self.list.poll_list(cx, &mut self.list_buf)? {
            //     Poll::Ready(n) => n,
            //     Poll::Pending => return Poll::Pending,
            // };
            // if n == 0 {
            //     return Poll::Ready(Ok(0));
            // }

            // let out_len = expanded.len();
            // // Extend `out` with only paths that pass the glob matcher.
            // expanded.extend(self.list_buf.drain(..).filter(|path| {
            //     let rel = &path[self.prefix_len..];
            //     self.matcher.is_match(rel)
            // }));
            // let append_count = expanded.len() - out_len;

            // if append_count > 0 {
            //     // We have paths, return them.
            //     return Poll::Ready(Ok(append_count));
            // }

            // // We filtered everything out. Read the next batch of paths.
            // continue;
        }
    }
}

impl<L> FileProvider for GlobList<L>
where
    L: DirList,
{
    fn poll_next(&mut self, cx: &mut Context, out: &mut Vec<String>) -> Poll<Result<usize>> {
        self.poll_expand(cx, out)
    }
}

#[derive(Debug)]
pub struct ExpandNext<'a, L: DirList> {
    glob: &'a mut GlobList<L>,
    expanded: &'a mut Vec<String>,
}

impl<'a, L> Future for ExpandNext<'a, L>
where
    L: DirList,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.glob.poll_expand(cx, this.expanded)
    }
}

#[derive(Debug)]
pub struct ExpandAll<'a, L: DirList> {
    glob: &'a mut GlobList<L>,
    expanded: &'a mut Vec<String>,
    total: usize,
}

impl<'a, L> Future for ExpandAll<'a, L>
where
    L: DirList,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let n = match this.glob.poll_expand(cx, this.expanded) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            this.total += n;
            if n == 0 {
                // We've read everything.
                return Poll::Ready(Ok(n));
            }

            // Continue... keep expanding.
        }
    }
}

/// If we should consider the given path a glob.
pub fn is_glob(path: &str) -> bool {
    path.contains(['*', '?', '[', '{'])
}

/// Split a glob like "data/2025-*/file-*.parquet" into ["data", "2025-*",
/// "file-*.parquet"]
fn split_segments<'a>(pattern: &'a str) -> Vec<&'a str> {
    // TODO: '\' on windows?
    pattern
        .trim_start_matches("./")
        .split('/')
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_segments_tests() {
        struct TestCase {
            input: &'static str,
            segments: Vec<&'static str>,
        }

        let cases = [
            TestCase {
                input: "*.parquet",
                segments: vec!["*.parquet"],
            },
            TestCase {
                input: "./*.parquet",
                segments: vec!["*.parquet"],
            },
            TestCase {
                input: "dir/**/file.parquet",
                segments: vec!["dir", "**", "file.parquet"],
            },
            TestCase {
                input: "data/2025-*/file-*.parquet",
                segments: vec!["data", "2025-*", "file-*.parquet"],
            },
        ];

        for case in cases {
            let segments: Vec<_> = split_segments(case.input);
            assert_eq!(case.segments, segments);
        }
    }
}
