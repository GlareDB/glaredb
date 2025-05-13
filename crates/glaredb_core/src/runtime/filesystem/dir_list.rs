use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result};

use super::FileType;

/// A single directory entry, either a "file" or a "sub directory".
#[derive(Debug, Clone)]
pub struct DirEntry {
    /// The full key/path, e.g. "foo/bar/baz.txt" or "foo/bar/"
    pub path: String,
    /// File type for this entry.
    pub file_type: FileType,
}

pub trait DirList: Debug + Sync + Send {
    /// List file paths, appending them to `paths`.
    ///
    /// Returns the number of paths appended. Returns 0 once there's nothing
    /// left to list.
    fn poll_list(&mut self, cx: &mut Context, ents: &mut Vec<DirEntry>) -> Poll<Result<usize>>;
}

#[derive(Debug)]
pub struct NotImplementedDirList;

impl DirList for NotImplementedDirList {
    fn poll_list(&mut self, _cx: &mut Context, _paths: &mut Vec<DirEntry>) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "Directory listing is not implemented for this filesystem",
        )))
    }
}

pub trait DirListExt: DirList {
    /// List the next set of files.
    ///
    /// Resolves to 0 once there are not more files to list.
    fn list<'a>(&'a mut self, paths: &'a mut Vec<DirEntry>) -> List<'a, Self> {
        List { list: self, paths }
    }

    /// List all files, appending them to `paths`.
    ///
    /// The future will resolve to the total number of items appended to the
    /// vec.
    fn list_all<'a>(&'a mut self, paths: &'a mut Vec<DirEntry>) -> ListAll<'a, Self> {
        ListAll {
            list: self,
            paths,
            total: 0,
        }
    }
}

impl<L> DirListExt for L where L: DirList {}

#[derive(Debug)]
pub struct List<'a, L: DirList + ?Sized> {
    list: &'a mut L,
    paths: &'a mut Vec<DirEntry>,
}

impl<'a, L> Future for List<'a, L>
where
    L: DirList + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.list.poll_list(cx, this.paths)
    }
}

#[derive(Debug)]
pub struct ListAll<'a, L: DirList + ?Sized> {
    list: &'a mut L,
    paths: &'a mut Vec<DirEntry>,
    total: usize,
}

impl<'a, L> Future for ListAll<'a, L>
where
    L: DirList + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        loop {
            let n = match this.list.poll_list(cx, this.paths) {
                Poll::Ready(Ok(n)) => n,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            };

            this.total += n;

            if n == 0 {
                // We listed everything.
                return Poll::Ready(Ok(this.total));
            }

            // Continue on to keep listing...
        }
    }
}
