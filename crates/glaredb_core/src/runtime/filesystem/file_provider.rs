use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::Result;

use super::FileSystemWithState;
use super::glob::is_glob;

/// Trait for providing files to read.
pub trait FileProvider: Debug + Sync + Send {
    /// Poll for the next batch of files.
    fn poll_next(&mut self, cx: &mut Context, out: &mut Vec<String>) -> Poll<Result<usize>>;
}

/// File provider that provides just a single file.
#[derive(Debug)]
pub struct SingleFileProvider {
    path: Option<String>,
}

impl SingleFileProvider {
    pub fn new(path: impl Into<String>) -> Self {
        SingleFileProvider {
            path: Some(path.into()),
        }
    }
}

impl FileProvider for SingleFileProvider {
    fn poll_next(&mut self, _cx: &mut Context, out: &mut Vec<String>) -> Poll<Result<usize>> {
        if let Some(path) = self.path.take() {
            out.push(path);
            Poll::Ready(Ok(1))
        } else {
            // "exhausted"
            Poll::Ready(Ok(0))
        }
    }
}

#[derive(Debug)]
pub struct MultiFileProvider {
    provider: Box<dyn FileProvider>,
    paths: Vec<String>,
    exhausted: bool,
}

impl MultiFileProvider {
    pub fn try_new_from_path(fs: &FileSystemWithState, path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        let provider = if is_glob(&path) {
            fs.glob_list_file_provider(&path)?
        } else {
            Box::new(SingleFileProvider::new(path))
        };

        Ok(MultiFileProvider {
            provider,
            paths: Vec::new(),
            exhausted: false,
        })
    }

    /// Get the 'n'th file path.
    pub fn poll_get_nth(&mut self, cx: &mut Context, n: usize) -> Poll<Result<Option<&str>>> {
        loop {
            if n < self.paths.len() {
                return Poll::Ready(Ok(Some(&self.paths[n])));
            }

            if self.exhausted {
                return Poll::Ready(Ok(None));
            }

            let appended = match self.provider.poll_next(cx, &mut self.paths) {
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

    /// Get the 'n'th path, fetching it from the underlying provider if needed.
    pub fn get_nth(&mut self, n: usize) -> GetNth {
        GetNth { provider: self, n }
    }
}

#[derive(Debug)]
pub struct GetNth<'a> {
    provider: &'a mut MultiFileProvider,
    n: usize,
}

impl<'a> Future for GetNth<'a> {
    type Output = Result<Option<String>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Clone the string here to avoid lifetime issues.
        //
        // We could make the MultiFileProvider pin, but... don't really want to.
        match this.provider.poll_get_nth(cx, this.n) {
            Poll::Ready(Ok(Some(s))) => Poll::Ready(Ok(Some(s.to_string()))),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(None)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
