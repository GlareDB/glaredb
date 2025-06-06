use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result};

use super::glob::is_glob;
use super::{FileOpenContext, FileSystemWithState};
use crate::arrays::datatype::DataType;
use crate::arrays::field::{ColumnSchema, Field};
use crate::arrays::scalar::{BorrowedScalarValue, ScalarValue};
use crate::functions::table::TableFunctionInput;
use crate::functions::table::scan::ScanContext;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

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
// TODO: Probably Arc<str>, or remove the Clone and make the change to creating
// partition states to also be provided the bind data.
#[derive(Debug, Clone)]
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

    pub fn expanded(&self) -> &[String] {
        &self.expanded
    }
}

/// Provider for reading multiple files. This should be used for all "file
/// scanning" functions.
///
/// Metadata columns:
///
/// - _filename: Name of the file as reported by the filesystem.
/// - _rowid: Index of the row relative to the file. This must be relative to
///   the entire file. For example, the _rowid in a parquet file should
///   indicate the row in the file, not the row index in just the row
///   groups we read.
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

    // TODO: Possibly doing a bit much?
    //
    // I wanted to pass in the filesystem directly, but to determine which
    // filesystem to use, we need access to one of the paths, which we don't
    // have until we get the underlying scalar value...
    pub async fn try_new_from_inputs(
        scan_context: ScanContext<'_>,
        input: &TableFunctionInput,
    ) -> Result<(Self, FileSystemWithState)> {
        let path = ConstFold::rewrite(input.positional[0].clone())?.try_into_scalar()?;

        match path {
            ScalarValue::Utf8(s) => {
                let s = s.into_owned();
                let fs = scan_context.dispatch.filesystem_for_path(&s)?;
                let context = FileOpenContext::new(scan_context.database_context, &input.named);
                let fs = fs.load_state(context).await?;

                let provider = if is_glob(&s) {
                    fs.read_glob(&s).await?
                } else {
                    Box::new(StaticFileProvider::new([s]))
                };

                let provider = MultiFileProvider {
                    provider,
                    exhausted: false,
                };

                Ok((provider, fs))
            }
            BorrowedScalarValue::List(list) => {
                // Currently does not support list of globs.
                //
                // If we wanted to support that, then we can implement an
                // additional file provider backed by multiple glob providers.
                // However we should think if it's worth trying to do parallel
                // expands in that case.
                //
                // Also semantically could be a bit confusing. Would something
                // like `['data/**/*.csv', 'data/path/*.csv']` read the same
                // files (yes but possibly surprising)?
                let paths = list
                    .into_iter()
                    .map(|s| s.try_into_string())
                    .collect::<Result<Vec<_>>>()?;

                let fs = match paths.first() {
                    Some(path) => {
                        // Determine which filesystem to use based on the first
                        // path.
                        let fs = scan_context.dispatch.filesystem_for_path(path)?;

                        // Now ensure that all paths can be handled by this
                        // filesystem.
                        for path in &paths[1..] {
                            if !fs.call_can_handle_path(path) {
                                return Err(DbError::new(format!(
                                    "{} file system cannot handle path '{}'",
                                    fs.name, path
                                )));
                            }
                        }

                        // TODO: We could possibly use multiple filesystems, but
                        // I think the use case needs to be strong before trying
                        // to support it.

                        let context =
                            FileOpenContext::new(scan_context.database_context, &input.named);
                        fs.load_state(context).await?
                    }
                    None => {
                        return Err(DbError::new(
                            "No file paths provided, cannot determine which filesystem to use",
                        ));
                    }
                };

                let provider = MultiFileProvider {
                    provider: Box::new(StaticFileProvider::new(paths)),
                    exhausted: false,
                };

                Ok((provider, fs))
            }
            other => Err(DbError::new(format!(
                "Cannot use {} as a file path. Provider either a string or list of strings.",
                other,
            ))),
        }
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

    #[must_use]
    pub fn expand_n<'a>(&'a mut self, data: &'a mut MultiFileData, n: usize) -> ExpandN<'a> {
        ExpandN {
            provider: self,
            data,
            n,
        }
    }

    #[must_use]
    pub fn expand_all<'a>(&'a mut self, data: &'a mut MultiFileData) -> ExpandAll<'a> {
        ExpandAll {
            provider: self,
            data,
            n: 1, // Start by trying expand a single path, then keep growing.
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
    n: usize,
}

impl Future for ExpandAll<'_> {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        while !this.provider.exhausted {
            let poll = this.provider.poll_expand_n(cx, this.data, this.n)?;
            match poll {
                Poll::Ready(_) => {
                    // Continually request and more until we're exhausted. The
                    // number we use here doesn't matter as long as it keeps
                    // going up.
                    this.n += 10;
                }
                Poll::Pending => return Poll::Pending,
            }

            // Continue... expanding.
        }

        Poll::Ready(Ok(()))
    }
}
