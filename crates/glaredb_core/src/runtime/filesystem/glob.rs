use std::task::{Context, Poll};

use glaredb_error::{DbError, OptionExt, Result, ResultExt};
use globset::{GlobBuilder, GlobMatcher};

use super::FileSystem;
use super::directory::{DirEntry, ReadDirHandle};
use super::file_provider::FileProvider;

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
        // TODO: For s3/gcs, we'll need to ensure we don't mess with the
        // schemes. Tbh might need to tweak the auto-detect stuff too.

        let mut segments = split_segments(glob);
        if segments.is_empty() {
            return Err(DbError::new("Missing segments for glob"));
        }

        // Find the root dir to use.
        let mut root = Vec::new();
        while !segments.is_empty() && !is_glob(segments[0]) {
            root.push(segments.remove(0));
        }

        // TODO: Probably needs to be os/filesystem specific.
        //
        // E.g. for s3, always use '/'. If local and windows, use '\'.
        let root = root.join("/");
        let root_dir = fs.read_dir(&root, state);

        // Build matchers per segment.
        let mut matchers = Vec::with_capacity(segments.len());
        for seg in &segments {
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

        let segments = segments.into_iter().map(|s| s.to_string()).collect();
        let stack = vec![(root_dir, 0)];

        Ok(GlobHandle {
            segments,
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

/// If we should consider the given path a glob.
pub fn is_glob(path: &str) -> bool {
    path.contains(['*', '?', '[', '{'])
}

/// Split a glob like "data/2025-*/file-*.parquet" into ["data", "2025-*",
/// "file-*.parquet"]
fn split_segments(pattern: &str) -> Vec<&str> {
    // TODO: '\' on windows?
    pattern.split('/').filter(|s| !s.is_empty()).collect()
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
                segments: vec![".", "*.parquet"],
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
