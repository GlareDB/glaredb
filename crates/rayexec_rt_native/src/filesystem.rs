use std::fs::{self, File as StdFile};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use rayexec_error::{Result, ResultExt};
use rayexec_io::exp::{AsyncReadStream, FileSource};

#[derive(Debug)]
pub struct LocalFile {
    len: usize,
    file: StdFile,
}

impl LocalFile {
    pub fn open_for_read(&self, path: impl AsRef<Path>) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path.as_ref())
            .context_fn(|| format!("failed to open file: {}", path.as_ref().to_string_lossy()))?;

        let len = file.metadata()?.len() as usize;

        Ok(LocalFile { len, file })
    }

    pub fn open_for_overwrite(&self, path: impl AsRef<Path>) -> Result<Self> {
        let file = fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path.as_ref())
            .context_fn(|| format!("failed to open file: {}", path.as_ref().to_string_lossy()))?;

        Ok(LocalFile { len: 0, file })
    }
}

impl FileSource for LocalFile {
    fn read(&mut self) -> Pin<Box<dyn AsyncReadStream>> {
        Box::pin(LocalFileRead::Seeking {
            file: self.file.try_clone().unwrap(),
            seek_to: 0,
            read_amount: self.len,
        })
    }

    fn read_range(&mut self, start: usize, len: usize) -> Pin<Box<dyn AsyncReadStream>> {
        Box::pin(LocalFileRead::Seeking {
            file: self.file.try_clone().unwrap(),
            seek_to: start as u64,
            read_amount: len,
        })
    }
}

#[derive(Debug)]
enum LocalFileRead {
    Seeking {
        file: StdFile,
        seek_to: u64,
        read_amount: usize,
    },
    Streaming {
        file: StdFile,
        remaining: usize,
    },
    Uninit,
}

impl AsyncReadStream for LocalFileRead {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context,
        mut buf: &mut [u8],
    ) -> Result<Poll<Option<usize>>> {
        let this = &mut *self;

        loop {
            match this {
                Self::Seeking { file, seek_to, .. } => {
                    file.seek(SeekFrom::Start(*seek_to))
                        .context_fn(|| format!("failed to seek to '{}' in file", seek_to))?;

                    let state = std::mem::replace(this, LocalFileRead::Uninit);

                    match state {
                        Self::Seeking {
                            file, read_amount, ..
                        } => {
                            *this = LocalFileRead::Streaming {
                                file,
                                remaining: read_amount,
                            }
                        }
                        _ => unreachable!(),
                    }

                    // Move to streaming.
                    continue;
                }
                Self::Streaming { file, remaining } => {
                    if *remaining == 0 {
                        return Ok(Poll::Ready(None));
                    }

                    if buf.len() > *remaining {
                        buf = &mut buf[0..*remaining];
                    }

                    let n = file.read(buf).context("failed to read file")?;
                    *remaining -= n;

                    return Ok(Poll::Ready(Some(n)));
                }
                Self::Uninit => panic!("invalid state"),
            }
        }
    }
}
