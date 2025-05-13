use std::fmt::Debug;
use std::pin::Pin;
use std::task::{Context, Poll};

use glaredb_error::{DbError, Result};

pub trait FileList: Debug + Sync + Send {
    /// List file paths, appending them to `paths`.
    ///
    /// Returns the number of paths appended. Returns 0 once there's nothing
    /// left to list.
    fn poll_list(&mut self, cx: &mut Context, paths: &mut Vec<String>) -> Poll<Result<usize>>;
}

#[derive(Debug)]
pub struct NotImplementedFileList;

impl FileList for NotImplementedFileList {
    fn poll_list(&mut self, _cx: &mut Context, _paths: &mut Vec<String>) -> Poll<Result<usize>> {
        Poll::Ready(Err(DbError::new(
            "File listing is not implemented for this filesystem",
        )))
    }
}

pub trait FileListExt: FileList {
    /// List the next set of files.
    ///
    /// Resolves to 0 once there are not more files to list.
    fn list<'a>(&'a mut self, paths: &'a mut Vec<String>) -> List<'a, Self> {
        List { list: self, paths }
    }

    /// List all files, appending them to `paths`.
    ///
    /// The future will resolve to the total number of items appended to the
    /// vec.
    fn list_all<'a>(&'a mut self, paths: &'a mut Vec<String>) -> ListAll<'a, Self> {
        ListAll {
            list: self,
            paths,
            total: 0,
        }
    }
}

impl<L> FileListExt for L where L: FileList {}

#[derive(Debug)]
pub struct List<'a, L: FileList + ?Sized> {
    list: &'a mut L,
    paths: &'a mut Vec<String>,
}

impl<'a, L> Future for List<'a, L>
where
    L: FileList + ?Sized,
{
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        this.list.poll_list(cx, this.paths)
    }
}

#[derive(Debug)]
pub struct ListAll<'a, L: FileList + ?Sized> {
    list: &'a mut L,
    paths: &'a mut Vec<String>,
    total: usize,
}

impl<'a, L> Future for ListAll<'a, L>
where
    L: FileList + ?Sized,
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
