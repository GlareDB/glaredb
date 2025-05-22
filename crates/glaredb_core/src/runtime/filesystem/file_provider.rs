use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::Result;

use super::FileSystemWithState;
use super::glob::is_glob;
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};

/// Trait for providing files to read.
pub trait FileProvider: Debug + Sync + Send {
    /// Poll for the next batch of files.
    fn poll_next(&mut self, cx: &mut Context, out: &mut Vec<String>) -> Poll<Result<usize>>;
}

/// File provider with a static list of file paths.
#[derive(Debug)]
pub struct StaticFileProvider {
    paths: Vec<String>,
}

impl StaticFileProvider {
    pub fn new<S>(paths: impl IntoIterator<Item = S>) -> Self
    where
        S: Into<String>,
    {
        StaticFileProvider {
            paths: paths.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl FileProvider for StaticFileProvider {
    fn poll_next(&mut self, _cx: &mut Context, out: &mut Vec<String>) -> Poll<Result<usize>> {
        let n = self.paths.len();
        out.append(&mut self.paths);
        Poll::Ready(Ok(n))
    }
}

/// Data associated with the multi-file provider.
#[derive(Debug)]
pub struct MultiFileData {
    expanded: Vec<String>,
}

impl MultiFileData {
    pub const fn empty() -> Self {
        MultiFileData {
            expanded: Vec::new(),
        }
    }

    pub fn expanded_count(&self) -> usize {
        self.expanded.len()
    }

    pub fn get(&self, n: usize) -> Option<&str> {
        self.expanded.get(n).map(|s| s.as_str())
    }
}

#[derive(Debug)]
pub struct MultiFileProvider {
    provider: Box<dyn FileProvider>,
    exhausted: bool,
}

impl MultiFileProvider {
    pub const META_PROJECTION_FILENAME: usize = 0;
    pub const META_PROJECTION_ROWID: usize = 1;

    pub fn meta_schema(&self) -> ColumnSchema {
        ColumnSchema::new([
            Field::new("_filename", DataType::utf8(), false),
            Field::new("_rowid", DataType::int64(), false),
        ])
    }

    pub fn try_new_from_path(fs: &FileSystemWithState, path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let provider = if is_glob(&path) {
            fs.read_glob(&path)?
        } else {
            Box::new(StaticFileProvider::new([path]))
        };

        Ok(MultiFileProvider {
            provider,
            exhausted: false,
        })
    }

    /// Expands at least `n` paths.
    ///
    /// If the provided data already has `n` paths, expanded, then this will
    /// immediately resolve.
    ///
    /// Returns the total number of paths we've expanded so according to the
    /// data. If the returned number is less than `n`, then we've completely
    /// expanded all paths.
    pub fn poll_expand_n(
        &mut self,
        cx: &mut Context,
        data: &mut MultiFileData,
        n: usize,
    ) -> Poll<Result<usize>> {
        loop {
            if n < data.expanded.len() {
                return Poll::Ready(Ok(data.expanded.len()));
            }

            if self.exhausted {
                return Poll::Ready(Ok(data.expanded.len()));
            }

            let appended = match self.provider.poll_next(cx, &mut data.expanded) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            if appended == 0 {
                // No more files to read.
                self.exhausted = true;
            }

            // Continue... we want to check if paths now contains the file we're
            // looking for. Or return None if we're now exhausted.
        }
    }

    pub fn expand_n<'a>(&'a mut self, data: &'a mut MultiFileData, n: usize) -> ExpandN<'a> {
        ExpandN {
            provider: self,
            data,
            n,
        }
    }

    pub fn expand_all<'a>(&'a mut self, data: &'a mut MultiFileData) -> ExpandAll<'a> {
        ExpandAll {
            provider: self,
            data,
        }
    }
}

#[derive(Debug)]
pub struct ExpandN<'a> {
    provider: &'a mut MultiFileProvider,
    data: &'a mut MultiFileData,
    n: usize,
}

impl Future for ExpandN<'_> {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.provider.poll_expand_n(cx, this.data, this.n)
    }
}

#[derive(Debug)]
pub struct ExpandAll<'a> {
    provider: &'a mut MultiFileProvider,
    data: &'a mut MultiFileData,
}

impl Future for ExpandAll<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while !this.provider.exhausted {
            // Arbitrary, doesn't really matter.
            const EXPAND: usize = 10;

            let poll = this.provider.poll_expand_n(cx, this.data, EXPAND)?;
            if poll.is_pending() {
                return Poll::Pending;
            }

            // Continue... expanding.
        }

        Poll::Ready(Ok(()))
    }
}
