pub mod dispatch;
pub mod file_ext;
pub mod file_list;
pub mod glob;
pub mod memory;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use file_ext::FileExt;
use file_list::FileList;
use glaredb_error::{DbError, Result};
use glob::GlobList;

use crate::arrays::scalar::ScalarValue;
use crate::catalog::context::DatabaseContext;
use crate::expr::Expression;
use crate::optimizer::expr_rewrite::ExpressionRewriteRule;
use crate::optimizer::expr_rewrite::const_fold::ConstFold;

pub trait File: Debug + Sync + Send + 'static {
    /// Get the path of this file.
    fn path(&self) -> &str;

    /// Get the size in bytes of this file.
    // TODO: Probably u64
    fn size(&self) -> usize;

    /// Read from the current position into the buffer, returning the number of
    /// bytes read.
    ///
    /// A return of Ok(0) indicates either an EOF or the provided buffer has a
    /// length of zero.
    fn poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>;

    /// Write at the current position, returning the number of bytes written.
    fn poll_write(&mut self, cx: &mut Context, buf: &[u8]) -> Poll<Result<usize>>;

    /// Seek the provided position in the file.
    fn poll_seek(&mut self, cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>>;

    /// Flush the file to disk (or persistent storage).
    fn poll_flush(&mut self, cx: &mut Context) -> Poll<Result<()>>;
}

#[derive(Debug)]
pub struct AnyFile {
    pub(crate) vtable: &'static RawFileVTable,
    pub(crate) file: Box<dyn Any + Sync + Send>,
}

impl AnyFile {
    pub fn from_file<F>(file: F) -> Self
    where
        F: File,
    {
        AnyFile {
            vtable: F::VTABLE,
            file: Box::new(file),
        }
    }

    pub fn call_path(&self) -> &str {
        (self.vtable.path_fn)(self.file.as_ref())
    }

    pub fn call_size(&self) -> usize {
        // TODO: We could probably just stick the size on the struct.
        (self.vtable.size_fn)(self.file.as_ref())
    }

    pub fn call_poll_read(&mut self, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>> {
        (self.vtable.poll_read_fn)(self.file.as_mut(), cx, buf)
    }

    pub fn call_poll_seek(&mut self, cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>> {
        (self.vtable.poll_seek_fn)(self.file.as_mut(), cx, seek)
    }

    pub fn call_read<'a>(&'a mut self, buf: &'a mut [u8]) -> FileSystemFuture<'a, Result<usize>> {
        (self.vtable.read_fn)(self.file.as_mut(), buf)
    }

    pub fn call_read_fill<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> FileSystemFuture<'a, Result<usize>> {
        (self.vtable.read_fill_fn)(self.file.as_mut(), buf)
    }

    pub fn call_read_exact<'a>(
        &'a mut self,
        buf: &'a mut [u8],
    ) -> FileSystemFuture<'a, Result<()>> {
        (self.vtable.read_exact_fn)(self.file.as_mut(), buf)
    }

    pub fn call_seek(&mut self, seek: io::SeekFrom) -> FileSystemFuture<'_, Result<()>> {
        (self.vtable.seek_fn)(self.file.as_mut(), seek)
    }
}

#[allow(clippy::type_complexity)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileVTable {
    path_fn: fn(&dyn Any) -> &str,
    size_fn: fn(&dyn Any) -> usize,
    poll_read_fn: fn(&mut dyn Any, cx: &mut Context, buf: &mut [u8]) -> Poll<Result<usize>>,
    poll_seek_fn: fn(&mut dyn Any, cx: &mut Context, seek: io::SeekFrom) -> Poll<Result<()>>,
    read_fn: for<'a> fn(&'a mut dyn Any, buf: &'a mut [u8]) -> FileSystemFuture<'a, Result<usize>>,
    read_fill_fn:
        for<'a> fn(&'a mut dyn Any, buf: &'a mut [u8]) -> FileSystemFuture<'a, Result<usize>>,
    read_exact_fn:
        for<'a> fn(&'a mut dyn Any, buf: &'a mut [u8]) -> FileSystemFuture<'a, Result<()>>,
    seek_fn: for<'a> fn(&'a mut dyn Any, seek: io::SeekFrom) -> FileSystemFuture<'a, Result<()>>,
}

trait FileVTable {
    const VTABLE: &'static RawFileVTable;
}

impl<F> FileVTable for F
where
    F: File,
{
    const VTABLE: &'static RawFileVTable = &RawFileVTable {
        path_fn: |file| {
            let file = file.downcast_ref::<Self>().unwrap();
            file.path()
        },

        size_fn: |file| {
            let file = file.downcast_ref::<Self>().unwrap();
            file.size()
        },

        poll_read_fn: |file, cx, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            file.poll_read(cx, buf)
        },

        poll_seek_fn: |file, cx, seek| {
            let file = file.downcast_mut::<Self>().unwrap();
            file.poll_seek(cx, seek)
        },

        read_fn: |file, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            Box::pin(file.read(buf))
        },

        read_fill_fn: |file, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            Box::pin(file.read_fill(buf))
        },

        read_exact_fn: |file, buf| {
            let file = file.downcast_mut::<Self>().unwrap();
            Box::pin(file.read_exact(buf))
        },

        seek_fn: |file, seek| {
            let file = file.downcast_mut::<Self>().unwrap();
            Box::pin(file.seek(seek))
        },
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileType {
    File,
    Directory,
}

impl FileType {
    pub const fn is_file(&self) -> bool {
        matches!(self, FileType::File)
    }

    pub const fn is_dir(&self) -> bool {
        matches!(self, FileType::Directory)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStat {
    pub file_type: FileType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenFlags(u16);

impl OpenFlags {
    pub const READ: OpenFlags = OpenFlags(1 << 0);
    pub const WRITE: OpenFlags = OpenFlags(1 << 1);
    pub const CREATE: OpenFlags = OpenFlags(1 << 2);

    pub fn new(flags: impl IntoIterator<Item = OpenFlags>) -> Self {
        let mut result = 0;
        for f in flags {
            result |= f.0;
        }
        OpenFlags(result)
    }

    pub const fn is_read(&self) -> bool {
        self.0 & Self::READ.0 != 0
    }

    pub const fn is_write(&self) -> bool {
        self.0 & Self::WRITE.0 != 0
    }

    pub const fn is_create(&self) -> bool {
        self.0 & Self::CREATE.0 != 0
    }
}

/// Context used for creating state needed by the file system to open files.
#[derive(Debug)]
pub struct FileOpenContext<'a> {
    #[allow(unused)] // TODO: db context will be used to lookup up stored config values
    db_context: &'a DatabaseContext,
    /// Named arguments provided to the scan function.
    named_arguments: &'a HashMap<String, Expression>,
}

impl<'a> FileOpenContext<'a> {
    pub fn new(
        db_context: &'a DatabaseContext,
        named_arguments: &'a HashMap<String, Expression>,
    ) -> Self {
        FileOpenContext {
            db_context,
            named_arguments,
        }
    }

    /// Get a value, returning Ok(None) if it doesn't exist.
    ///
    /// May return an error if the value exists, but the expression representing
    /// the value isn't a constant.
    // TODO: There's going to need to be a way to namespace keys here, since we
    // wouldn't want to just look up "key_id" in the catalog since that could
    // apply to many things.
    pub fn get_value(&self, key: &str) -> Result<Option<ScalarValue>> {
        self.named_arguments
            .get(key)
            .map(|expr| ConstFold::rewrite(expr.clone())?.try_into_scalar())
            .transpose()
    }

    /// Get a value, erroring if either the value doesn't exist, or it's not a
    /// constant scalar value.
    pub fn require_value(&self, key: &str) -> Result<ScalarValue> {
        self.get_value(key)?
            .ok_or_else(|| DbError::new(format!("Missing named argument '{key}'")))
    }
}

pub trait FileSystem: Debug + Sync + Send + 'static {
    // TODO: Probably remove this and just return `AnyFile` from open.
    //
    // This would allow us to return different kinds of file handles depending
    // on the open flags used.
    type File: File;

    /// Extra state used when opening or statting a single file.
    type State: Sync + Send;

    /// File list used for prefix listing.
    type FileList: FileList;

    fn state_from_context(&self, context: FileOpenContext) -> Result<Self::State>;

    /// Open a file at a given path.
    fn open(
        &self,
        flags: OpenFlags,
        path: &str,
        state: &Self::State,
    ) -> impl Future<Output = Result<Self::File>> + Sync + Send;

    /// Stat the file.
    ///
    /// This should return Ok(None) if the request was successful, but the file
    /// doesn't exist.
    fn stat(
        &self,
        path: &str,
        state: &Self::State,
    ) -> impl Future<Output = Result<Option<FileStat>>> + Sync + Send;

    fn list_prefix(&self, prefix: &str, state: &Self::State) -> Self::FileList;

    fn list_glob(&self, glob: &str, state: &Self::State) -> Result<GlobList<Self::FileList>> {
        GlobList::try_new(glob, self, state)
    }

    /// Returns if this filesystem is able to handle the provided path.
    fn can_handle_path(&self, path: &str) -> bool;
}

/// A boxed future.
pub type FileSystemFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Sync + Send + 'a>>;

#[derive(Debug, Clone)]
pub(crate) struct AnyState(pub Arc<dyn Any + Sync + Send>);

#[derive(Debug, Clone)]
pub struct FileSystemWithState {
    pub(crate) fs: AnyFileSystem,
    pub(crate) state: AnyState,
}

impl FileSystemWithState {
    /// Opens a files at the given path.
    pub fn open<'a>(
        &'a self,
        flags: OpenFlags,
        path: &'a str,
    ) -> FileSystemFuture<'a, Result<AnyFile>> {
        (self.fs.vtable.open_fn)(
            self.fs.filesystem.as_ref(),
            flags,
            path,
            self.state.0.as_ref(),
        )
    }

    /// Like `call_open`, but returns a static boxed future.
    // TODO: I don't really want to do this, but I also don't care enough to
    // whip out unsafe since it's just for opening a file.
    pub fn open_static(
        &self,
        flags: OpenFlags,
        path: impl Into<String>,
    ) -> FileSystemFuture<'static, Result<AnyFile>> {
        (self.fs.vtable.open_static_fn)(self.fs.clone(), flags, path.into(), self.state.clone())
    }

    pub fn stat<'a>(&'a self, path: &'a str) -> FileSystemFuture<'a, Result<Option<FileStat>>> {
        (self.fs.vtable.stat_fn)(self.fs.filesystem.as_ref(), path, self.state.0.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct AnyFileSystem {
    pub(crate) vtable: &'static RawFileSystemVTable,
    pub(crate) filesystem: Arc<dyn Any + Sync + Send>,
}

impl AnyFileSystem {
    pub fn from_filesystem<F>(fs: F) -> Self
    where
        F: FileSystem,
    {
        AnyFileSystem {
            vtable: F::VTABLE,
            filesystem: Arc::new(fs),
        }
    }

    pub fn try_with_context(&self, context: FileOpenContext) -> Result<FileSystemWithState> {
        let state = self.call_state_from_context(context)?;
        Ok(FileSystemWithState {
            fs: self.clone(),
            state,
        })
    }

    pub fn call_can_handle_path(&self, path: &str) -> bool {
        (self.vtable.can_handle_path_fn)(self.filesystem.as_ref(), path)
    }

    fn call_state_from_context(&self, context: FileOpenContext) -> Result<AnyState> {
        (self.vtable.state_from_context_fn)(self.filesystem.as_ref(), context)
    }
}

#[allow(clippy::type_complexity)] // I do know how this is a complex type, but but that's kinda the point.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RawFileSystemVTable {
    state_from_context_fn:
        for<'a> fn(fs: &dyn Any, context: FileOpenContext<'a>) -> Result<AnyState>,

    open_fn: for<'a> fn(
        fs: &'a dyn Any,
        flags: OpenFlags,
        path: &'a str,
        state: &'a dyn Any,
    ) -> FileSystemFuture<'a, Result<AnyFile>>,

    open_static_fn: fn(
        fs: AnyFileSystem,
        flags: OpenFlags,
        path: String,
        state: AnyState,
    ) -> FileSystemFuture<'static, Result<AnyFile>>,

    stat_fn: for<'a> fn(
        fs: &'a dyn Any,
        path: &'a str,
        state: &'a dyn Any,
    ) -> FileSystemFuture<'a, Result<Option<FileStat>>>,

    can_handle_path_fn: fn(fs: &dyn Any, path: &str) -> bool,
}

trait FileSystemVTable {
    const VTABLE: &'static RawFileSystemVTable;
}

impl<S> FileSystemVTable for S
where
    S: FileSystem,
{
    const VTABLE: &'static RawFileSystemVTable = &RawFileSystemVTable {
        state_from_context_fn: |fs, context| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            let state = fs.state_from_context(context)?;
            Ok(AnyState(Arc::new(state)))
        },

        open_fn: |fs, flags, path, state| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            let state = state.downcast_ref::<S::State>().unwrap();
            Box::pin(async move {
                let file = fs.open(flags, path, state).await?;
                Ok(AnyFile::from_file(file))
            })
        },

        open_static_fn: |any_fs, flags, path, state| {
            Box::pin(async move {
                let fs = any_fs.filesystem.downcast_ref::<Self>().unwrap();
                let state = state.0.downcast_ref::<S::State>().unwrap();
                let file = fs.open(flags, &path, state).await?;
                Ok(AnyFile::from_file(file))
            })
        },

        stat_fn: |fs, path, state| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            let state = state.downcast_ref::<S::State>().unwrap();
            Box::pin(async { fs.stat(path, state).await })
        },

        can_handle_path_fn: |fs, path| {
            let fs = fs.downcast_ref::<Self>().unwrap();
            fs.can_handle_path(path)
        },
    };
}
