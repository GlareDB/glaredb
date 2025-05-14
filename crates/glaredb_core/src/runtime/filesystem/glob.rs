use std::task::{Context, Poll};

use glaredb_error::{OptionExt, Result, ResultExt};
use globset::{GlobBuilder, GlobMatcher};

use super::FileSystem;
use super::directory::{DirEntry, ReadDirHandle};
use super::file_provider::FileProvider;

/// If we should consider the given path a glob.
pub fn is_glob(path: &str) -> bool {
    path.contains(['*', '?', '[', '{'])
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobSegments {
    /// The "root" directory of the glob.
    ///
    /// This should be the largest prefix path of the glob that itself does not
    /// contain any glob characters.
    ///
    /// `root_dir` will be passed into the filesystem's `read_dir` method to
    /// begin directory traversal, and should include the full path (e.g. the
    /// path would be passed to `open`). That means for object stores, the root
    /// dir will be something like `s3://bucket/dir/nested`.
    pub root_dir: String,
    /// The glob segments for each segment in the path.
    ///
    /// This is essentially taking the remaining path (after getting the root
    /// dir), and splitting on '/' (or some other path sep). No additional
    /// processing should be done. Literal segments (segments containing no glob
    /// chars) will be handled appropriately.
    pub segments: Vec<String>,
}

#[derive(Debug)]
pub struct GlobHandle<D: ReadDirHandle> {
    /// The raw segmens, e.g. ["foo", "*", "bar-*.txt"]
    segments: Vec<String>,
    /// A matcher per segment, Some if contains glob, None if literal.
    matchers: Vec<Option<GlobMatcher>>,
    /// Working stack.
    stack: Vec<(D, usize)>,
    /// Scratch buffer for each poll_list.
    buf: Vec<DirEntry>,
}

impl<D> GlobHandle<D>
where
    D: ReadDirHandle,
{
    pub fn try_new<F>(fs: &F, state: &F::State, glob: &str) -> Result<Self>
    where
        F: FileSystem<ReadDirHandle = D> + ?Sized,
    {
        let glob_segments = F::glob_segments(glob)?;

        // Build matchers per segment.
        let mut matchers = Vec::with_capacity(glob_segments.segments.len());
        for seg in &glob_segments.segments {
            if is_glob(seg) {
                let matcher = GlobBuilder::new(seg)
                    .literal_separator(true)
                    .build()
                    .context("Failed to build glob for segment")?
                    .compile_matcher();
                matchers.push(Some(matcher));
            } else {
                matchers.push(None);
            }
        }

        let root_dir = fs.read_dir(&glob_segments.root_dir, state)?;
        let stack = vec![(root_dir, 0)];

        Ok(GlobHandle {
            segments: glob_segments.segments,
            matchers,
            stack,
            buf: Vec::new(),
        })
    }

    pub fn poll_expand(
        &mut self,
        cx: &mut Context,
        expanded: &mut Vec<String>,
    ) -> Poll<Result<usize>> {
        let expanded_len = expanded.len();

        loop {
            let (handle, seg_idx) = match self.stack.last_mut() {
                Some((handle, idx)) => (handle, *idx),
                None => {
                    // Stack empty, we're done.
                    return Poll::Ready(Ok(0));
                }
            };

            // TODO: Might need to track when we clear this if `poll_list` will
            // ever use it as a scratch buffer.
            self.buf.clear();

            match handle.poll_list(cx, &mut self.buf)? {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(0) => {
                    // Done listing this dir, pop and move on.
                    self.stack.pop();
                    continue;
                }
                Poll::Ready(_) => {
                    let is_last = seg_idx + 1 == self.segments.len();
                    let pat = &self.segments[seg_idx];
                    let is_double_star = pat == "**";
                    let m_opt = &self.matchers[seg_idx];

                    // TODO: Avoid.
                    let mut temp_stack = Vec::new();

                    for ent in self.buf.drain(..) {
                        // Only match the final path component.
                        let name = ent.path.rsplit('/').next().required("last path segment")?;

                        if is_double_star {
                            // Always "match" the dir to recurse deeper
                            if ent.file_type.is_dir() {
                                // Consume one directory but stay on the same '**' segment:
                                let child = handle.change_dir(name)?;
                                temp_stack.push((child, seg_idx));

                                // Also allow skipping the '**' altogether and
                                // moving to the next segment:
                                if seg_idx + 1 < self.segments.len() {
                                    let child2 = handle.change_dir(name)?;
                                    temp_stack.push((child2, seg_idx + 1));
                                }
                            }

                            // And we don’t emit anything at this stage unless
                            // '**' is the last segmentt, in which case
                            // "everything under here" is a match.
                            if seg_idx + 1 == self.segments.len() && ent.file_type.is_file() {
                                // TODO: Same as below, optionally emit
                                // directories?
                                expanded.push(ent.path.clone());
                            }

                            continue;
                        }

                        let ok = if let Some(m) = m_opt {
                            // Glob matching.
                            m.is_match(name)
                        } else {
                            // Literal segment.
                            name == pat
                        };
                        if !ok {
                            continue;
                        }

                        if is_last {
                            // Final segment, emit path
                            if ent.file_type.is_file() {
                                expanded.push(ent.path.clone());
                            }
                            // TODO: Optionally emit dir?
                        } else if ent.file_type.is_dir() {
                            // not last, cd into next dir.
                            let child = handle.change_dir(name)?;
                            temp_stack.push((child, seg_idx + 1));
                        }
                    }

                    self.stack.append(&mut temp_stack);

                    let appended = expanded.len() - expanded_len;
                    if appended > 0 {
                        return Poll::Ready(Ok(appended));
                    }

                    // Otherwise we filtered everything out... continue on.
                }
            }
        }
    }
}

impl<D> FileProvider for GlobHandle<D>
where
    D: ReadDirHandle,
{
    fn poll_next(&mut self, cx: &mut Context, out: &mut Vec<String>) -> Poll<Result<usize>> {
        self.poll_expand(cx, out)
    }
}
